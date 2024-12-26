import os
import re
from copy import deepcopy

# import locale
from lxml import etree

from .constants import MAKE_TESTS_HAPPY, NAMESPACES, VALID_SEPARATORS

PARTFILENAME_RE = re.compile(r"([A-Za-z_]+)(\d+).xml")


class MergeHeaderFooterDocument(object):
    """prepare and merge one Header/Footer document for merge_templates

    helper class to handle the actual merging of one header/footer document
    It handles Header and Footer relation documents, for which you have to
    create copies of documents and update relations.
    """

    def __init__(self, part_info, relations, separator):
        self.part_content_type = part_info["file"]
        self.zi = part_info["zi"]
        self.part = part_info["part"]
        self.relations = relations
        self.sep_type = None
        self.target, self.id_type, self.part_id = self._parse_part_filename(self.zi.filename)
        self.new_parts = []  # list of (filename, root) parts
        self._current_part = None
        self.has_fields = bool(self.part.findall(".//MergeField"))
        self._prepare_data(separator)

    def _parse_part_filename(self, filename):
        filename = os.path.basename(filename)
        match = PARTFILENAME_RE.match(filename)
        assert match
        return (filename, *match.groups())

    def _prepare_data(self, separator):
        if separator not in VALID_SEPARATORS:
            raise ValueError("Invalid separator argument")
        self.sep_type, _sep_class = separator.split("_")

    def prepare(self, merge_data, first=False):
        if self.has_fields:
            self._current_part = deepcopy(self.part)

    def merge(self, merge_data, row, first=False):
        """Merges one row into the current prepared body"""
        if self.has_fields:
            assert self._current_part is not None
            merge_data.replace(self._current_part.getroot(), row)

    def finish(self, merge_data, abort=False):
        """finishes the current merge, by updating the relations"""

        if abort:  # for skipping the record
            self._current_part = None

        if self._current_part is not None:
            # @TODO use the existing header/footers for the first section
            new_id = merge_data.unique_id_manager.register_id(self.id_type)
            new_target = self.target.replace(self.part_id, str(new_id))
            new_filename = self.zi.filename.replace(self.part_id, str(new_id))
            new_part_content_type = deepcopy(self.part_content_type)
            new_part_content_type.attrib["PartName"] = self.part_content_type.attrib["PartName"].replace(
                self.target, new_target
            )
            self.new_parts.append((new_filename, new_part_content_type, self._current_part))
            self._current_part = None
            return [(self.target, new_target)]

        return []


class MergeDocument(object):
    """prepare and merge one document

    helper class to handle the actual merging of one document
    with multiple rows of data
    It is not compatible with Footer, Header, Footnotes, Endnotes XML documents.
    It prepares the body, sections, separators
    """

    def __init__(self, merge_data, root, relations, separator):
        self.merge_data = merge_data
        self.root = root
        self.relations = relations
        # self.sep_type = sep_type
        # self.sep_class = sep_class
        # if sep_class == 'section':
        #     self._set_section_type()

        # self._last_section = None  # saving the last section to add it at the end
        # self._body = None  # the document body, where all the documents are appended
        # self._body_copy = None  # a deep copy of the original body without ending section
        self._current_body = None  # the current document body where all the changes are merged
        # self._current_separator = None
        self._finish_rels = []
        self._prepare_data(separator)

    def _prepare_data(self, separator):
        if separator not in VALID_SEPARATORS:
            raise ValueError("Invalid separator argument")
        sep_type, sep_class = separator.split("_")

        # TODO why setting the type only to the first section and not to the last section ?

        if sep_class == "section":
            # FINDING FIRST SECTION OF THE DOCUMENT
            first_section = self.root.find("w:body/w:p/w:pPr/w:sectPr", namespaces=NAMESPACES)
            if first_section is None:
                first_section = self.root.find("w:body/w:sectPr", namespaces=NAMESPACES)

            type_element = first_section.find("w:type", namespaces=NAMESPACES)

            if MAKE_TESTS_HAPPY:
                if type_element is not None:
                    first_section.remove(type_element)
                    type_element = None

            if type_element is None:
                type_element = etree.SubElement(first_section, "{%(w)s}type" % NAMESPACES, attrib=None, nsmap=None)

            type_element.set("{%(w)s}val" % NAMESPACES, sep_type)

        # FINDING LAST SECTION OF THE DOCUMENT
        self._last_section = self.root.find("w:body/w:sectPr", namespaces=NAMESPACES)

        self._body = self._last_section.getparent()
        self._body.remove(self._last_section)
        self._last_section = deepcopy(self._last_section)  # fix a bug

        self._body_copy = deepcopy(self._body)

        # EMPTY THE BODY - PREPARE TO FILL IT WITH DATA
        self._body.clear()

        self._separator = etree.Element("{%(w)s}p" % NAMESPACES, attrib=None, nsmap=None)

        if sep_class == "section":
            pPr = etree.SubElement(self._separator, "{%(w)s}pPr" % NAMESPACES, attrib=None, nsmap=None)
            pPr.append(deepcopy(self._last_section))
        elif sep_class == "break":
            r = etree.SubElement(self._separator, "{%(w)s}r" % NAMESPACES, attrib=None, nsmap=None)
            nbreak = etree.SubElement(r, "{%(w)s}br" % NAMESPACES, attrib=None, nsmap=None)
            nbreak.set("{%(w)s}type" % NAMESPACES, sep_type)

    def prepare(self, merge_data, first=False):
        """prepares the current body for the merge"""
        assert self._current_body is None
        # add separator if not the first document
        if not first:
            # @TODO replace the relation references in the full body, not only in the
            # separator
            # @TODO refactor the whole preparation process, so it is straightforward
            # and doesn't look like a hack
            for old_target, new_target in self._finish_rels:
                self.replace_relation_reference(merge_data, old_target, new_target)
            self._body.append(self._current_separator)
        self._current_separator = deepcopy(self._separator)
        self._current_body = deepcopy(self._body_copy)
        merge_data.fix_ids(self._current_body)

    def merge(self, merge_data, row, first=False):
        """Merges one row into the current prepared body"""

        merge_data.replace(self._current_body, row)

    def replace_relation_reference(self, merge_data, old_target, new_target, sep=None):
        # assert self._current_body is not None
        if sep is None:
            sep = self._current_separator

        old_relation = self.relations.get_relation_elem(old_target)
        new_rel_id = self.relations.replace_relation(merge_data, old_relation, new_target)

        for elem in sep.xpath('//*[@r:id="%s"]' % old_relation.attrib["Id"], namespaces=NAMESPACES):
            elem.attrib["{%(r)s}id" % NAMESPACES] = new_rel_id

    def finish(self, finish_rels, abort=False):
        """finishes the current body by saving it into the main body or into a file (future feature)"""
        self._finish_rels = finish_rels
        if abort:  # for skipping the record
            self._current_body = None

        if self._current_body is not None:
            for child in self._current_body:
                self._body.append(child)
            self._current_body = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            # self.finish(True)
            for old_target, new_target in self._finish_rels:
                self.replace_relation_reference(self.merge_data, old_target, new_target, sep=self._last_section)
            self._body.append(self._last_section)

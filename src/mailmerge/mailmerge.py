import os
import warnings
from copy import deepcopy

# import locale
from zipfile import ZIP_DEFLATED, ZipFile

from lxml import etree

from .constants import CONTENT_TYPES_PARTS, MAKE_TESTS_HAPPY, NAMESPACES
from .field import SimpleMergeField, SkipRecord
from .mergedata import MergeData
from .part import MergeDocument, MergeHeaderFooterDocument
from .rels import RelationsDocument


class MailMerge(object):
    """
    MailMerge class to write an output docx document by merging data rows to a template

    The class uses the builtin MergeFields in Word. There are two kind of data fields, simple and complex.
    http://officeopenxml.com/WPfields.php
    The MERGEFIELD can have MERGEFORMAT
    MERGEFIELD can be nested inside other "complex" fields, in which case those fields should be updated
    in the saved docx

    MailMerge implements this by finding all Fields and replacing them with placeholder Elements of type
    MergeElement

    Those MergeElement elements will then be replaced for each run with a list of elements containing run
    elements with texts.
    The MergeElement value (list of run Elements) should be computed recursively for the inner MergeElements

    """

    def __init__(
        self,
        file,
        remove_empty_tables=False,
        auto_update_fields_on_open="no",
        keep_fields="none",
    ):
        """
        auto_update_fields_on_open : no, auto, always - auto = only when needed
        keep_fields : none - merge all fields even if no data, some - keep fields with no data, all - keep all fields
        """
        self.zip = ZipFile(file)
        self.zip_is_closed = False
        self.parts = {}  # zi_part: ElementTree
        self.new_parts = []  # list of [(filename, part)]
        self.categories = {}  # category: [zi, ...]
        self.merge_data = MergeData(remove_empty_tables=remove_empty_tables, keep_fields=keep_fields)
        self.remove_empty_tables = remove_empty_tables
        self.auto_update_fields_on_open = auto_update_fields_on_open
        self.keep_fields = keep_fields
        self._has_unmerged_fields = False

        try:
            self.__fill_parts()

            for part_info in self.get_parts():
                self.__fill_simple_fields(part_info["part"])
                self.__fill_complex_fields(part_info["part"])

        except Exception:
            self.zip.close()
            raise

    def get_parts(self, categories=None):
        """return all the parts based on categories"""
        if categories is None:
            categories = ["main", "header_footer", "notes"]
        elif isinstance(categories, str):
            categories = [categories]
        return [self.parts[zi] for category in categories for zi in self.categories.get(category, [])]

    def get_settings(self):
        """returns the settings part"""
        return self.parts[self.categories["settings"][0]]["part"]

    def get_content_types(self):
        """ " returns the content types part"""
        return self.parts[self.categories["content_types"][0]]["part"]

    def get_relations(self, part_zi):
        """returns the"""
        rel_fn = "word/_rels/%s.rels" % os.path.basename(part_zi.filename)
        if rel_fn in self.zip.namelist():
            zi = self.zip.getinfo(rel_fn)
            rel_root = etree.parse(self.zip.open(zi))
            self.parts[zi] = dict(zi=zi, part=rel_root)
            relations = RelationsDocument(rel_root)
            for relation in relations.get_all():
                self.merge_data.unique_id_manager.register_id_str(relation.attrib["Id"])
            return relations
        # else:
        #     print(rel_fn, self.zip.namelist())

    def __setattr__(self, __name, __value):
        super(MailMerge, self).__setattr__(__name, __value)
        if __name == "remove_empty_tables":
            self.merge_data.remove_empty_tables = __value

    def __fill_parts(self):
        content_types_zi = self.zip.getinfo("[Content_Types].xml")
        content_types = etree.parse(self.zip.open(content_types_zi))
        self.categories["content_types"] = [content_types_zi]
        self.parts[content_types_zi] = dict(part=content_types)
        for file in content_types.findall("{%(ct)s}Override" % NAMESPACES):
            part_type = file.attrib["ContentType" % NAMESPACES]
            category = CONTENT_TYPES_PARTS.get(part_type)
            if category:
                zi, self.parts[zi] = self.__get_tree_of_file(file)
                self.categories.setdefault(category, []).append(zi)

    def __fill_simple_fields(self, part):
        for fld_simple_elem in part.findall(".//{%(w)s}fldSimple" % NAMESPACES):
            first_run_elem = deepcopy(fld_simple_elem.find("{%(w)s}r" % NAMESPACES))
            if MAKE_TESTS_HAPPY:
                first_run_elem.clear()
            merge_field_obj = self.merge_data.make_data_field(
                fld_simple_elem.getparent(),
                instr=fld_simple_elem.get("{%(w)s}instr" % NAMESPACES),
                field_class=SimpleMergeField,
                all_elements=[fld_simple_elem],
                instr_elements=[first_run_elem],
                show_elements=[first_run_elem],
            )
            if merge_field_obj:
                merge_field_obj.insert_into_tree()

    def __get_next_element(self, current_element):
        """returns the next element of a complex field"""
        next_element = current_element.getnext()
        current_paragraph = current_element.getparent()
        # we search through paragraphs for the next <w:r> element
        while next_element is None:
            current_paragraph = current_paragraph.getnext()
            if current_paragraph is None:
                return None, None, None
            next_element = current_paragraph.find("w:r", namespaces=NAMESPACES)

        # print(''.join(next_element.xpath('w:instrText/text()', namespaces=NAMESPACES)))
        field_char_subelem = next_element.find("w:fldChar", namespaces=NAMESPACES)
        if field_char_subelem is None:
            return next_element, None, None

        return (
            next_element,
            field_char_subelem,
            field_char_subelem.xpath("@w:fldCharType", namespaces=NAMESPACES)[0],
        )

    def _pull_next_merge_field(self, elements_of_type_begin, nested=False):
        assert elements_of_type_begin
        current_element = elements_of_type_begin.pop(0)
        parent_element = current_element.getparent()
        all_elements = []  # we need all the elments in case of updates
        instr_elements = []  # the instruction part, elements that define how to get the value
        show_elements = []  # the elements showing the current value

        current_element_list = instr_elements
        all_elements.append(current_element)

        # good_elements = []
        # ignore_elements = [current_element]
        # current_element_list = good_elements
        field_char_type = None

        # print('>>>>>>>')
        while field_char_type != "end":
            # find next sibling
            next_element, field_char_subelem, field_char_type = self.__get_next_element(current_element)

            if next_element is None:
                instr_text = self.merge_data.get_instr_text(instr_elements, recursive=True)
                raise ValueError("begin without end near:" + instr_text)

            if field_char_type == "begin":
                # nested elements
                assert elements_of_type_begin[0] is next_element
                merge_field_sub_obj, next_element = self._pull_next_merge_field(elements_of_type_begin, nested=True)
                if merge_field_sub_obj:
                    next_element = merge_field_sub_obj.insert_into_tree()
                # print("current list is ignore", current_element_list is ignore_elements)
                # print("<<<<< #####", etree.tostring(next_element))
            elif field_char_type == "separate":
                current_element_list = show_elements
            elif next_element.tag == "MergeField":
                # we have a nested simple Field - mark it as nested
                self.merge_data.mark_field_as_nested(next_element.get("merge_key"))

            if field_char_type not in ["end", "separate"]:
                current_element_list.append(next_element)
            all_elements.append(next_element)
            current_element = next_element

        # print('<<<<<<<', len(good_elements), len(ignore_elements))
        merge_obj = self.merge_data.make_data_field(
            parent_element,
            nested=nested,
            all_elements=all_elements,
            instr_elements=instr_elements,
            show_elements=show_elements,
        )
        return merge_obj, current_element

    def __fill_complex_fields(self, part):
        """finds all begin fields and then builds the MergeField objects and inserts the replacement
        Elements in the tree"""
        # will find all "runs" containing an element of fldChar type=begin
        elements_of_type_begin = list(
            part.findall('.//{%(w)s}r/{%(w)s}fldChar[@{%(w)s}fldCharType="begin"]/..' % NAMESPACES)
        )
        while elements_of_type_begin:
            merge_field_obj, _ = self._pull_next_merge_field(elements_of_type_begin)
            if merge_field_obj:
                # print(merge_field_obj.instr)
                merge_field_obj.insert_into_tree()

    def __fix_settings(self):
        settings = self.get_settings()
        if settings:
            settings_root = settings.getroot()
            if not self._has_unmerged_fields:
                mail_merge = settings_root.find("{%(w)s}mailMerge" % NAMESPACES)
                if mail_merge is not None:
                    settings_root.remove(mail_merge)

            add_update_fields_setting = (
                self.auto_update_fields_on_open == "auto"
                and self.merge_data.has_nested_fields
                or self.auto_update_fields_on_open == "always"
            )
            if add_update_fields_setting:
                update_fields_elem = settings_root.find("{%(w)s}updateFields" % NAMESPACES)
                if not update_fields_elem:
                    update_fields_elem = etree.SubElement(
                        settings_root, "{%(w)s}updateFields" % NAMESPACES, attrib=None, nsmap=None
                    )
                update_fields_elem.set("{%(w)s}val" % NAMESPACES, "true")

    def __get_tree_of_file(self, file):
        fn = file.attrib["PartName" % NAMESPACES].split("/", 1)[1]
        zi = self.zip.getinfo(fn)
        return zi, dict(zi=zi, file=file, part=etree.parse(self.zip.open(zi)))

    def write(self, file, empty_value=""):
        self._has_unmerged_fields = bool(self.get_merge_fields())

        if empty_value is not None:
            if self.keep_fields == "none":
                # we use empty values to replace all fields having no data
                self.merge(**{field: empty_value for field in self.get_merge_fields()})
            else:
                # we keep the fields having no data with the original value
                self.merge_data.replace_fields_with_missing_data = True
                self.merge()
                self.merge_data.replace_fields_with_missing_data = False

        # Remove mail merge settings to avoid error messages when opening document in Winword
        self.__fix_settings()

        # add the new files in the content types
        content_types = self.get_content_types().getroot()
        for new_part in self.new_parts:
            content_types.append(new_part.part_content_type)

        with ZipFile(file, "w", ZIP_DEFLATED) as output:
            for zi in self.zip.filelist:
                if zi in self.parts:
                    xml = etree.tostring(
                        self.parts[zi]["part"].getroot(),
                        encoding="UTF-8",
                        xml_declaration=True,
                    )
                    output.writestr(zi.filename, xml)
                else:
                    output.writestr(zi.filename, self.zip.read(zi))

            for new_part in self.new_parts:
                xml = etree.tostring(new_part.content.getroot(), encoding="UTF-8", xml_declaration=True)
                output.writestr(new_part.path, xml)
                # TODO add relations

    def get_merge_fields(self):
        """ " get the fields from the document"""
        return self._get_merge_fields()

    def _get_merge_fields(self, parts=None):
        if not parts:
            parts = self.get_parts()

        fields = set()
        for part in parts:
            for mf in part["part"].findall(".//MergeField"):
                fields.add(mf.attrib["name"])
                # for name in self.merge_data.get_merge_fields(mf.attrib['merge_key']):
                #     fields.add(name)
        return fields

    def merge_templates(self, replacements, separator):
        """mailmerge one document with MULTIPLE data sets, and separate the output

        is NOT compatible with header/footer/footnotes/endnotes
        separator must be :
        - page_break : Page Break.
        - column_break : Column Break. ONLY HAVE EFFECT IF DOCUMENT HAVE COLUMNS
        - textWrapping_break : Line Break.
        - continuous_section : Continuous section break. Begins the section on the next paragraph.
        - evenPage_section : evenPage section break. section begins on the next even-numbered page, leaving the next
            odd page blank if necessary.
        - nextColumn_section : nextColumn section break. section begins on the following column on the page.
            ONLY HAVE EFFECT IF DOCUMENT HAVE COLUMNS
        - nextPage_section : nextPage section break. section begins on the following page.
        - oddPage_section : oddPage section break. section begins on the next odd-numbered page, leaving the next even
            page blank if necessary.
        """
        assert replacements, "empty data"
        # TYPE PARAM CONTROL AND SPLIT

        # prepare the side documents, like headers, footers, etc
        rel_docs = []
        for part_info in self.get_parts(["header_footer"]):
            relations = self.get_relations(part_info["zi"])
            merge_header_footer_doc = MergeHeaderFooterDocument(part_info, relations, separator)
            rel_docs.append(merge_header_footer_doc)
            self.merge_data.unique_id_manager.register_id(
                merge_header_footer_doc.id_type, int(merge_header_footer_doc.part_id)
            )

        # Duplicate template. Creates a copy of the template, does a merge, and separates them by a new paragraph,
        # a new break or a new section break.

        # GET ROOT - WORK WITH DOCUMENT
        for part_info in self.get_parts(["main"]):
            root = part_info["part"].getroot()
            relations = self.get_relations(part_info["zi"])

            # the mailmerge is done with the help of the MergeDocument class
            # that handles the document duplication
            with MergeDocument(self.merge_data, root, relations, separator) as merge_doc:
                row = self.merge_data.start_merge(replacements)
                while row is not None:
                    merge_doc.prepare(self.merge_data, first=self.merge_data.is_first())

                    finish_rels = []
                    for rel_doc in rel_docs:
                        rel_doc.prepare(self.merge_data, first=self.merge_data.is_first())
                        rel_doc.merge(self.merge_data, row)
                        finish_rels.extend(rel_doc.finish(self.merge_data))

                    try:
                        merge_doc.merge(self.merge_data, row)
                        merge_doc.finish(finish_rels)
                    except SkipRecord:
                        merge_doc.finish(finish_rels, abort=True)

                    row = self.merge_data.next_row()

        # add all new files in the zip
        for rel_doc in rel_docs:
            self.new_parts.extend(rel_doc.new_parts)

    def merge_pages(self, replacements):
        """
        Deprecated method.
        """
        warnings.warn(
            "merge_pages has been deprecated in favour of merge_templates",
            category=DeprecationWarning,
            stacklevel=2,
        )
        self.merge_templates(replacements, "page_break")

    def merge(self, **replacements):
        """mailmerge one document with one set of values

        is compatible with header/footer/footnotes/endnotes
        """
        self._merge(replacements)

    def _merge(self, replacements):
        for part_info in self.get_parts():
            self.merge_data.replace(part_info["part"], replacements)

        for new_part in self.new_parts:
            self.merge_data.replace(new_part.content, replacements)

    def merge_rows(self, anchor, rows):
        """anchor is one of the fields in the table"""

        for part_info in self.get_parts():
            self.merge_data.replace_table_rows(part_info["part"], anchor, rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if not self.zip_is_closed:
            try:
                self.zip.close()
            finally:
                self.zip_is_closed = True

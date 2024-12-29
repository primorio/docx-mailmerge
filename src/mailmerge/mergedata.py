import shlex
import warnings
from copy import deepcopy

from .conditional_field import IfField, NextIfField, SkipIfField
from .constants import NAMESPACES, TAGS_WITH_ID
from .field import MergeField, NextField, NextRecord
from .unique_man import UniqueIdsManager


class MergeData(object):
    """prepare the MergeField objects and the data"""

    SUPPORTED_FIELDS = {"MERGEFIELD", "NEXT", "NEXTIF", "SKIPIF"}
    EXPERIMENTAL_FIELDS = {"IF"}
    FIELD_CLASSES = {"NEXT": NextField, "IF": IfField, "NEXTIF": NextIfField, "SKIPIF": SkipIfField}

    def __init__(self, settings):
        self._merge_field_map = {}  # merge_field.key: MergeField()
        self._merge_field_next_id = 0
        self.unique_id_manager = UniqueIdsManager()
        self.has_nested_fields = False
        self.settings = settings
        self.replace_fields_with_missing_data = False
        self._rows = None
        self._current_index = None
        self.experimental_fields = set() if not settings.enable_experimental else self.EXPERIMENTAL_FIELDS

    def start_merge(self, replacements):
        assert self._rows is None, "merge already started"
        self._rows = replacements
        return self.next_row()

    def next_row(self):
        assert self._rows is not None, "merge not yet started"

        if self._current_index is None:
            self._current_index = 0
        else:
            self._current_index += 1

        if self._current_index < len(self._rows):
            return self._rows[self._current_index]

    def is_first(self):
        return self._current_index == 0

    def get_new_element_id(self, element):
        """Returns None if the existing id is new otherwise a new id"""
        # tag = element.tag
        elem_id = element.get("id")
        if elem_id is None:
            return None
        elem_id = int(elem_id)
        new_id = self.unique_id_manager.register_id("id", elem_id)
        if new_id:
            return str(new_id)
        return None

    # def get_merge_fields(self, key):
    #     merge_obj = self.get_field_obj(key)
    #     if merge_obj.name:
    #         yield merge_obj.name

    def get_instr_text(self, elements, recursive=False):
        texts = []
        current_parent = None
        for elem in elements:
            parent = elem.getparent()
            if current_parent is None:
                current_parent = parent
            elif current_parent != parent:
                current_parent = parent
                texts.append("\n")
            for text in elem.xpath("w:instrText/text()", namespaces=NAMESPACES):
                texts.append(text)

            for obj_name in elem.xpath("@merge_key"):
                if recursive:
                    texts.append(self.get_field_obj(obj_name).instr)
                else:
                    texts.append("{{{}}}".format(obj_name))

        return "".join(texts)

    @classmethod
    def _get_instr_tokens(cls, instr):
        s = shlex.shlex(instr, posix=True)
        s.whitespace_split = True
        s.commenters = ""
        s.escape = ""
        return s

    @classmethod
    def _get_field_type(cls, instr):
        s = shlex.split(instr, posix=False)
        # For elements with no instr, otherwise this will throw an exception
        if len(s) > 0:
            return s[0], s[1:]
        return "", []

    def make_data_field(
        self,
        parent,
        field_class=MergeField,
        key=None,
        nested=False,
        instr=None,
        all_elements=None,
        instr_elements=None,
        show_elements=None,
        **kwargs,
    ):
        """MergeField factory method"""
        if key is None:
            key = self._get_next_key()

        instr = instr or self.get_instr_text(instr_elements)
        field_type, rest = self._get_field_type(instr)
        if field_type not in self.SUPPORTED_FIELDS and field_type not in self.experimental_fields:
            # ignore the field
            # print("ignore field", instr)
            return None
        field_class = self.FIELD_CLASSES.get(field_type, field_class)

        try:
            tokens = list(self._get_instr_tokens(instr))
        except ValueError as e:
            tokens = [field_type] + list(map(lambda part: part.replace('"', ""), rest))
            warnings.warn("Invalid field description <{}> near: <{}>".format(str(e), instr))

        # print("make data object", field_class, instr, len(elements), len(kwargs.get('ignore_elements', [])))
        field_obj = field_class(
            parent,
            key=key,
            instr=instr,
            nested=nested,
            instr_tokens=tokens,
            all_elements=all_elements,
            instr_elements=instr_elements,
            show_elements=show_elements,
            **kwargs,
        )

        assert key not in self._merge_field_map
        if nested:
            self.has_nested_fields = True
        self._merge_field_map[key] = field_obj
        return field_obj

    def get_field_obj(self, key):
        return self._merge_field_map[key]

    def mark_field_as_nested(self, key, nested=True):
        if nested:
            self.has_nested_fields = True
        self.get_field_obj(key).nested = nested

    def _get_next_key(self):
        key = "field_{}".format(self._merge_field_next_id)
        self._merge_field_next_id += 1
        return key

    def replace(self, body, row):
        """replaces in the body xml tree the MergeField elements with values from the row"""
        all_tables = {key: value for key, value in row.items() if isinstance(value, list)}

        for anchor, table_rows in all_tables.items():
            self.replace_table_rows(body, anchor, table_rows)

        merge_fields = body.findall(".//MergeField")
        for field_element in merge_fields:
            field_obj = None
            try:
                field_obj = self.get_field_object(field_element, row)
                field_obj.reset()
                if self._has_value_in_row(field_element, row):
                    field_obj.fill_data(self, row)  # can throw NextRecord
                    self.replace_field(field_element, field_obj)
                elif self.replace_fields_with_missing_data:
                    self.replace_field(field_element, field_obj, force_keep_field=True)
            except NextRecord:
                self.replace_field(field_element, field_obj)
                row = self.next_row()

    def _has_value_in_row(self, field_element, row):
        return not (field_element.get("name") and (row is None or field_element.get("name") not in row))

    def replace_field(self, field_element, field_obj=None, force_keep_field=False):
        """replaces a field element MergeField in the body with the filled_elements"""
        # assert len(filled_field.filled_elements) == 1
        if field_obj:
            keep_field = force_keep_field or self.settings.keep_fields == "all"
            elements_to_replace = field_obj.get_elements_to_replace(keep_field=keep_field)
            for text_element in reversed(elements_to_replace):
                field_element.addnext(text_element)
        field_element.getparent().remove(field_element)

    def replace_table_rows(self, body, anchor, rows):
        """replace the rows of a table with the values from the rows list"""
        for table, idx, template in self.__find_row_anchor(body, anchor):
            if len(rows) > 0:
                del table[idx]
                for i, row_data in enumerate(rows):
                    row = deepcopy(template)
                    self.replace(row, row_data)
                    table.insert(idx + i, row)
            else:
                # if there is no data for a given table
                # we check whether table needs to be removed
                if self.settings.remove_empty_tables:
                    parent = table.getparent()
                    parent.remove(table)

    def __find_row_anchor(self, body, field):
        for table in body.findall(".//{%(w)s}tbl" % NAMESPACES):
            for idx, row in enumerate(table):
                if row.find('.//MergeField[@name="%s"]' % field) is not None:
                    yield table, idx, row

    def get_field_object(self, field_element, row):
        """ " fills the corresponding MergeField python object with data from row"""
        # if field_element.get('name') and (row is None or field_element.get('name') not in row):
        #     return None
        field_key = field_element.get("merge_key")
        field_obj = self._merge_field_map[field_key]
        return field_obj

    def fix_id(self, element, attr_gen):
        """will replace an id with a new unique id"""
        new_id = self.get_new_element_id(element)
        if new_id is not None:
            element.attrib["id"] = new_id
            for attr_name, attr_value in attr_gen.items():
                element.attrib[attr_name] = attr_value.format(id=new_id)

    def fix_ids(self, current_body):
        """will fix all ids in the current body"""
        for tag, attr_gen in TAGS_WITH_ID.items():
            for elem in current_body.xpath("//{}".format(tag), namespaces=NAMESPACES):
                self.fix_id(elem, attr_gen)

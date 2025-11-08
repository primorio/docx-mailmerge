import tempfile
import unittest
from os import path

from mailmerge import NAMESPACES, MailMerge
from tests.utils import EtreeMixin, get_document_body_part


class MergeNestedTableRowsTest(EtreeMixin, unittest.TestCase):
    def setUp(self):
        self.document = MailMerge(path.join(path.dirname(__file__), "test_merge_nested_table_rows.docx"))

    def test_merge_rows(self):
        self.assertEqual(
            self.document.get_merge_fields(),
            {
                "name",
                "version",
                "note",
                "desc",
            },
        )

        self.document.merge(
            name=[
                {"name": "Jon", "version": "1", "note": "A", "desc": "Hey"},
                {"name": "Snow", "version": "2", "note": "A+", "desc": "Wow"},
            ],
        )

        if True:
            with tempfile.TemporaryFile() as outfile:
                self.document.write(outfile)
        else:
            with open("tests/output/test_output_merge_nested_table_rows.docx", "wb") as outfile:
                self.document.write(outfile)

        root_elem = get_document_body_part(self.document).getroot()
        self.assertEqual(len(root_elem.findall(".//{%(w)s}tbl" % NAMESPACES)), 2)

    def tearDown(self):
        self.document.docx.close()

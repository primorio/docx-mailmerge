import unittest

from mailmerge import NAMESPACES

from tests.utils import EtreeMixin


class BeforeAfterWithParTest(EtreeMixin, unittest.TestCase):
    def test_number(self):
        values = ["one", "two", "three"]
        document, root_elem = self.merge_templates(
            "test_field_with_paragraph.docx",
            [
                {
                    "fieldname": value,
                }
                for value in values
            ],
            separator="nextPage_section",
            # output="tests/output/test_output_field_with_paragraph.docx",
        )

        fields = root_elem.xpath("//w:t/text()", namespaces=NAMESPACES)
        expected = [v for value in values for v in ["before", f"par{value} after ", "par"]]
        self.assertListEqual(fields, expected)

import unittest

from mailmerge import NAMESPACES

from tests.utils import EtreeMixin


class IfTest(EtreeMixin, unittest.TestCase):
    """
    Testing next records
    """

    def test_if_record(self):
        """
        Tests the IF record with paragraphs
        """
        values = ["one", "two", "three", "four", "five"]
        document, root_elem = self.merge_templates(
            "test_if_with_paragraph.docx",
            [{"fieldname": value} for value in values],
            mm_kwargs=dict(enable_experimental=True),
            # output="tests/test_output_next_record.docx"
        )

        self.assertFalse(root_elem.xpath("//MergeField", namespaces=NAMESPACES))
        fields = root_elem.xpath("//w:t/text()", namespaces=NAMESPACES)
        expected = [
            v for value in values for v in [value, f"{value}" + (" is " if value == "one" else " is not "), "one"]
        ]
        self.assertListEqual(fields, expected)

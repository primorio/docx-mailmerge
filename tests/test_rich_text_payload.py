import tempfile
import unittest
from os import path

from lxml import etree

from mailmerge import MailMerge, NAMESPACES, RichTextPayload
from tests.utils import get_document_body_part


class RichTextPayloadTest(unittest.TestCase):
    TEMPLATE = "test_multiple_elements.docx"

    def _paragraph(self, text):
        paragraph = etree.Element("{%(w)s}p" % NAMESPACES)
        run = etree.SubElement(paragraph, "{%(w)s}r" % NAMESPACES)
        text_node = etree.SubElement(run, "{%(w)s}t" % NAMESPACES)
        text_node.text = text
        return paragraph

    def _run(self, text):
        run = etree.Element("{%(w)s}r" % NAMESPACES)
        text_node = etree.SubElement(run, "{%(w)s}t" % NAMESPACES)
        text_node.text = text
        return run

    def test_block_level_payload_replaces_paragraph(self):
        payload = RichTextPayload(
            [self._paragraph("First paragraph"), self._paragraph("Second paragraph")],
        )
        with MailMerge(path.join(path.dirname(__file__), self.TEMPLATE)) as document:
            document.merge(foo=payload, bar="two", gak="three")

            with tempfile.TemporaryFile() as outfile:
                document.write(outfile)

            body = get_document_body_part(document).getroot()

        texts = [
            text
            for text in body.xpath("w:body/w:p/w:r/w:t/text()", namespaces=NAMESPACES)
            if text.strip()
        ]
        self.assertEqual(["First paragraph", "Second paragraph", "two", "three"], texts)

    def test_inline_payload_keeps_paragraph(self):
        payload = RichTextPayload([self._run("Inline value")], block_level=False)
        with MailMerge(path.join(path.dirname(__file__), self.TEMPLATE)) as document:
            document.merge(foo=payload, bar="two", gak="three")

            with tempfile.TemporaryFile() as outfile:
                document.write(outfile)

            body = get_document_body_part(document).getroot()

        first_paragraph_text = " ".join(
            body.xpath("w:body/w:p[1]//w:t/text()", namespaces=NAMESPACES)
        ).strip()
        self.assertIn("Inline value", first_paragraph_text)
        self.assertIn(
            "two",
            " ".join(
                body.xpath("w:body/w:p[2]//w:t/text()", namespaces=NAMESPACES)
            ).strip(),
        )

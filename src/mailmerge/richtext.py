from __future__ import annotations

from collections.abc import Iterable
from copy import deepcopy
from typing import List

from lxml import etree


class RichTextPayload:
    """Container for pre-rendered WordprocessingML fragments.

    The payload holds a list of lxml ``Element`` objects that represent the
    already-formatted content that should replace a merge field.  Elements can
    be block-level nodes (e.g. ``w:p`` paragraphs, ``w:tbl`` tables) or inline
    runs (``w:r``).  Set ``block_level`` to ``False`` when supplying inline
    content so that the merge algorithm keeps the surrounding paragraph in
    place.
    """

    __slots__ = ("_elements", "block_level")

    def __init__(self, elements: Iterable[etree._Element], *, block_level: bool = True):
        self.block_level = block_level
        self._elements = tuple(self._validate_elements(elements))

    @staticmethod
    def _validate_elements(elements: Iterable[etree._Element]) -> List[etree._Element]:
        validated: List[etree._Element] = []
        for element in elements:
            if not isinstance(element, etree._Element):
                raise TypeError(
                    "RichTextPayload elements must be lxml.etree._Element instances"
                )
            validated.append(element)
        return validated

    def clone_elements(self) -> List[etree._Element]:
        """Return deep copies of the stored elements, ready to be inserted."""

        return [deepcopy(element) for element in self._elements]

    def __bool__(self) -> bool:  # pragma: no cover - delegated to __len__
        return bool(self._elements)

    def __len__(self) -> int:
        return len(self._elements)

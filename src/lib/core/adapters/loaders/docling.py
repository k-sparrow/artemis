# core
import logging
from typing import List, Tuple

# third party
from docling.datamodel.document import DoclingDocument

__all__ = [
    "split_pages",
    "PAGE_BREAK_PLACEHOLDER",
]

logger = logging.getLogger(__name__)

# Sentinel inserted at page transitions by ``export_to_markdown`` and split on to
# reconstruct per-page Markdown. Deliberately HTML-comment-shaped and namespaced
# so it can never collide with real document content.
PAGE_BREAK_PLACEHOLDER = "<!-- ARTEMIS_PAGE_BREAK -->"


def split_pages(dl_doc: DoclingDocument) -> List[Tuple[int, str]]:
    """Reconstruct per-page Markdown from a single whole-document export.

    Calls ``export_to_markdown(page_break_placeholder=…)`` once (O(N+L)) and
    splits on the placeholder, rather than looping ``export_to_markdown(page_no=n)``
    per page (O(P·N)). Each segment is paired with a page number from the
    document's sorted page index.

    A page with no exportable content emits no transition, so the placeholder
    count can be *less* than the page count; ``zip`` against the sorted page
    numbers truncates to the shorter side and we log when they disagree so the
    positional mapping skew is observable.
    """
    markdown = dl_doc.export_to_markdown(page_break_placeholder=PAGE_BREAK_PLACEHOLDER)
    page_nos = sorted(dl_doc.pages)
    if not page_nos:
        # Non-paginated formats (Markdown/HTML/text) carry no page index, but they
        # still have content — treat the whole document as a single page so it gets
        # a parent. Chunks from these formats have ``page_no=None`` (no provenance)
        # and link to this page via the indexer's first-page fallback. Empty content
        # yields no page at all.
        whole = markdown.replace(PAGE_BREAK_PLACEHOLDER, "").strip()
        return [(1, whole)] if whole else []
    segments = markdown.split(PAGE_BREAK_PLACEHOLDER)
    if len(segments) != len(page_nos):
        logger.warning(
            "page_split_count_mismatch: %d segments vs %d pages",
            len(segments),
            len(page_nos),
        )
    return [(page_no, segment.strip()) for page_no, segment in zip(page_nos, segments)]

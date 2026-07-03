from io import BytesIO

from pypdf import PdfReader, PdfWriter

__all__ = ["pdf_page_count", "split_pdf_shards"]


def pdf_page_count(content: bytes) -> int:
    """Return the number of pages in a PDF without rendering any page."""
    return len(PdfReader(BytesIO(content)).pages)


def split_pdf_shards(content: bytes, page_limit: int) -> list[bytes]:
    """Split PDF bytes into ≤ page_limit-page chunks in document order.

    Returns a list of serialised PDF bytes. Intended to be uploaded to MinIO
    so docling-serve can process them sequentially via S3Source, keeping peak
    GPU memory bounded to one shard at a time.
    """
    reader = PdfReader(BytesIO(content))
    shards: list[bytes] = []
    for start in range(0, len(reader.pages), page_limit):
        writer = PdfWriter()
        for page in reader.pages[start : (start + page_limit)]:  # noqa: E203
            writer.add_page(page)
        buf = BytesIO()
        writer.write(buf)
        shards.append(buf.getvalue())
    return shards

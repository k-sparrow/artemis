import base64
from typing import Annotated

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from src.backend.mcp.api import client
from src.backend.mcp.api.settings import settings

__all__ = ["mcp"]

mcp = FastMCP("artemis")


def _owner_headers() -> dict[str, str]:
    return {"X-Owner-Id": settings.STUB_OWNER_ID}


# ---------------------------------------------------------------------------
# Storage tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Namespaces",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    )
)
async def list_namespaces(ctx: Context) -> list[dict]:
    """List all namespaces accessible to the current user.

    Returns the user's own private namespaces plus all shared (organisation-wide)
    namespaces. Each entry includes id, name, type (PRIVATE or SHARED), and
    created_at. Use the name field to identify namespaces — it is the human-readable
    label shown in the UI. Pass the id to retrieve(), list_objects(), or upload_file().
    """
    await ctx.info("list_namespaces called")
    resp = await client.storage_client.get("/namespaces", headers=_owner_headers())
    resp.raise_for_status()
    return resp.json()


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Namespace",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    )
)
async def get_namespace(
    namespace_id: Annotated[str, Field(description="UUID of the namespace to fetch.")],
    ctx: Context,
) -> dict:
    """Get details of a single namespace by its UUID.

    Returns the full namespace record: id, name, type, owner_id, created_at,
    updated_at. Call list_namespaces() first if you do not know the namespace_id.
    """
    await ctx.info(f"get_namespace called: namespace_id={namespace_id}")
    resp = await client.storage_client.get(
        f"/namespaces/{namespace_id}", headers=_owner_headers()
    )
    resp.raise_for_status()
    return resp.json()


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Objects",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    )
)
async def list_objects(
    namespace_id: Annotated[
        str, Field(description="UUID of the namespace to list objects from.")
    ],
    group_id: Annotated[
        str | None,
        Field(
            description=(
                "Optional group UUID to filter results. A group represents a logical "
                "ownership boundary such as a data source connector or an upload session. "  # noqa: E501
                "Omit to return all objects in the namespace."
            )
        ),
    ] = None,
    ctx: Context = None,
) -> list[dict]:
    """List objects (documents) stored in a namespace.

    Each object represents an uploaded and indexed file. Returns obj_id, filename,
    content_type, size_bytes, group_id, and ingestion status per object.
    Use group_id to narrow results to a specific connector or upload batch.
    """
    await ctx.info(
        f"list_objects called: namespace_id={namespace_id} group_id={group_id}"
    )
    params = {}
    if group_id:
        params["group_id"] = group_id
    resp = await client.storage_client.get(
        f"/namespaces/{namespace_id}/objects",
        params=params,
        headers=_owner_headers(),
    )
    resp.raise_for_status()
    return resp.json()


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get Object",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    )
)
async def get_object(
    namespace_id: Annotated[
        str, Field(description="UUID of the namespace containing the object.")
    ],
    obj_id: Annotated[
        str, Field(description="UUID of the object to fetch metadata for.")
    ],
    ctx: Context = None,
) -> dict:
    """Get metadata for a single document object in a namespace.

    Returns obj_id, filename, content_type, size_bytes, group_id, and ingestion
    status. Does not return file content — use retrieve() to search the indexed text.
    """
    await ctx.info(f"get_object called: namespace_id={namespace_id} obj_id={obj_id}")
    resp = await client.storage_client.get(
        f"/namespaces/{namespace_id}/objects/{obj_id}", headers=_owner_headers()
    )
    resp.raise_for_status()
    return resp.json()


@mcp.tool(
    annotations=ToolAnnotations(
        title="Upload File",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=False,
    )
)
async def upload_file(
    namespace_id: Annotated[
        str,
        Field(description="UUID of the target namespace. Must exist before uploading."),
    ],
    filename: Annotated[
        str,
        Field(description="Original filename including extension, e.g. 'report.pdf'."),
    ],
    content_base64: Annotated[str, Field(description="Base64-encoded file content.")],
    content_type: Annotated[
        str,
        Field(
            description=(
                "MIME type of the file, e.g. 'application/pdf' or 'text/markdown'. "
                "Defaults to 'application/octet-stream'."
            )
        ),
    ] = "application/octet-stream",
    ctx: Context = None,
) -> dict:
    """Upload a file into a namespace for asynchronous parsing and indexing.

    Content must be base64-encoded. The system parses and embeds the document
    asynchronously — the returned task_id can be used to poll ingestion status.
    Supported formats include PDF, Markdown, plain text, and DOCX.
    """
    await ctx.info(
        f"upload_file called: namespace_id={namespace_id} filename={filename}"
    )
    content = base64.b64decode(content_base64)
    resp = await client.storage_client.post(
        f"/namespaces/{namespace_id}/objects",
        files={"file": (filename, content, content_type)},
        headers=_owner_headers(),
    )
    resp.raise_for_status()
    return resp.json()


@mcp.tool(
    annotations=ToolAnnotations(
        title="List Groups",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    )
)
async def list_groups(
    namespace_id: Annotated[
        str, Field(description="UUID of the namespace to inspect.")
    ],
    ctx: Context = None,
) -> list[dict]:
    """List distinct groups within a namespace with object counts.

    Groups are logical ownership boundaries — typically a data source connector
    (enterprise) or an upload session (private). Returns [{group_id, object_count}].
    Use group_id values with list_objects() or retrieve() to scope operations.
    """
    await ctx.info(f"list_groups called: namespace_id={namespace_id}")
    resp = await client.storage_client.get(
        f"/namespaces/{namespace_id}/groups", headers=_owner_headers()
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Retrieval tools
# ---------------------------------------------------------------------------


@mcp.tool(
    annotations=ToolAnnotations(
        title="Retrieve",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=False,
    )
)
async def retrieve(
    namespace_id: Annotated[
        str,
        Field(
            description=(
                "UUID of the namespace to search in. "
                "Call list_namespaces() to discover available namespaces."
            )
        ),
    ],
    query: Annotated[
        str, Field(description="Natural-language question or search query.")
    ],
    top_k: Annotated[
        int, Field(description="Number of chunks to return.", ge=1, le=20)
    ] = 5,
    group_id: Annotated[
        str | None,
        Field(
            description=(
                "Optional group UUID to restrict search to a specific data source "
                "connector's documents. Omit to search the entire namespace."
            )
        ),
    ] = None,
    doc_id: Annotated[
        str | None,
        Field(
            description=(
                "Optional document UUID to restrict search to chunks from a single file. "
                "Useful for focused Q&A over a specific document."
            )
        ),
    ] = None,
    ctx: Context = None,
) -> list[dict]:
    """Search for relevant document chunks using hybrid semantic + BM25 retrieval.

    Performs dense vector similarity search combined with BM25 keyword matching over
    indexed documents in a namespace. Returns the top-k most relevant chunks with
    page_content, source filename, and relevance score. Use this tool to answer
    questions grounded in documents stored in Artemis.
    """
    await ctx.info(
        f"retrieve called: namespace={namespace_id} query={query!r} k={top_k}"
    )
    configurable: dict = {
        "namespace_id": namespace_id,
        "k": top_k,
    }
    if group_id:
        configurable["group_id"] = group_id
    if doc_id:
        configurable["doc_id"] = doc_id

    body = {"input": query, "config": {"configurable": configurable}}
    resp = await client.indexing_client.post("/retrieve/invoke", json=body)
    resp.raise_for_status()
    return [
        {
            "content": d["page_content"],
            "source": d["metadata"].get("source"),
            "score": d["metadata"].get("score"),
        }
        for d in resp.json()["output"]
    ]

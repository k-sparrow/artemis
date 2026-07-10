import base64
from typing import Annotated

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import Field

from src.backend.mcp.api import client
from src.backend.mcp.api.settings import settings

__all__ = ["mcp"]

# DNS rebinding protection is delegated to APISIX at the gateway layer.
# Disabling it here lets the server accept requests from any upstream host
# (internal Docker names, external domains) without an explicit allowlist.
mcp = FastMCP(
    "artemis",
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


def _owner_headers() -> dict[str, str]:
    return {"X-Owner-Id": settings.STUB_OWNER_ID}


def _trim_object(obj: dict) -> dict:
    """Drop fields that are redundant or carry no signal for an MCP caller.

    namespace_id: the caller already supplied it to make the request.
    object_type: hardcoded to "file" in every ingestion path today.
    id -> obj_id: matches the parameter name every other tool uses to
    reference an object (get_object(obj_id=...), etc.).
    """
    return {
        "obj_id": obj["id"],
        "source": obj["source"],
        "content_type": obj["content_type"],
        "size_bytes": obj["size_bytes"],
        "group_id": obj["group_id"],
        "ingested_at": obj["ingested_at"],
    }


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

    Each object represents an uploaded and indexed file. Returns obj_id, source,
    content_type, size_bytes, group_id, and ingested_at per object.
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
    return [_trim_object(obj) for obj in resp.json()]


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

    Returns obj_id, source, content_type, size_bytes, group_id, and ingested_at.
    Does not return file content — use retrieve() to search the indexed text.
    """
    await ctx.info(f"get_object called: namespace_id={namespace_id} obj_id={obj_id}")
    resp = await client.storage_client.get(
        f"/namespaces/{namespace_id}/objects/{obj_id}", headers=_owner_headers()
    )
    resp.raise_for_status()
    return _trim_object(resp.json())


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


if settings.ENABLE_UPLOAD:
    mcp.tool(
        annotations=ToolAnnotations(
            title="Upload File",
            readOnlyHint=False,
            destructiveHint=False,
            idempotentHint=False,
            openWorldHint=False,
        )
    )(upload_file)


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
    return_parents: Annotated[
        bool,
        Field(
            description=(
                "If true, return the full parent page containing each matched chunk "
                "instead of the chunk itself — more context, fewer/coarser results "
                "(duplicate chunks from the same page collapse to one page). "
                "score is typically null in this mode since it's a chunk-level signal."
            )
        ),
    ] = False,
    ctx: Context = None,
) -> list[dict]:
    """Search for relevant document chunks using hybrid semantic + BM25 retrieval.

    Performs dense vector similarity search combined with BM25 keyword matching over
    indexed documents in a namespace. Returns the top-k most relevant chunks with
    page_content, source filename, and relevance score. Use this tool to answer
    questions grounded in documents stored in Artemis. Set return_parents=true to get
    the full parent page around each hit instead of the chunk, when you need more
    surrounding context.
    """
    await ctx.info(
        f"retrieve called: namespace={namespace_id} query={query!r} k={top_k} "
        f"return_parents={return_parents}"
    )
    configurable: dict = {
        "namespace_id": namespace_id,
        "k": top_k,
    }
    if group_id:
        configurable["group_id"] = group_id
    if doc_id:
        configurable["doc_id"] = doc_id
    if return_parents:
        configurable["return_parents"] = True

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


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------


@mcp.resource(
    "artemis://about",
    name="About Artemis",
    title="What is Artemis?",
    description="Overview of the Artemis RAG system: concepts, workflow, and tool map.",
    mime_type="text/markdown",
)
def about_artemis() -> str:
    """Static overview so a client unfamiliar with Artemis can orient itself."""
    return """\
# Artemis

Artemis is a multi-tenant RAG (Retrieval-Augmented Generation) document \
ingestion and retrieval system. It stores documents, indexes them for \
search, and answers questions grounded in that indexed text.

## Concepts

- **Namespace**: the top-level container for documents. `PRIVATE` namespaces \
belong to a single user; `SHARED` namespaces are organisation-wide.
- **Group**: a logical boundary within a namespace — typically a data source \
connector (enterprise ingestion) or a single upload session (private \
uploads). Use it to scope listing/search to one batch or connector.
- **Object**: one ingested document (a file). Listed via `list_objects()`, \
fetched via `get_object()`.

## Ingestion is asynchronous

`upload_file()` returns a `task_id` immediately after the bytes are \
accepted — it does not wait for parsing/embedding to finish. A document is \
not yet retrievable right after upload; there is currently no MCP tool to \
poll ingestion status, so allow some time before querying newly uploaded \
content.

## Retrieval

`retrieve()` performs hybrid search — dense vector similarity plus BM25 \
keyword matching — over the indexed chunks of a namespace. Narrow it with \
`group_id` or `doc_id`. Set `return_parents=true` to get the full page \
around each matched chunk instead of the chunk itself, when more \
surrounding context is needed.

## Suggested call order

1. `list_namespaces()` — discover what's available.
2. `list_objects()` / `list_groups()` — see what's ingested in a namespace.
3. `retrieve()` — search and answer questions grounded in that content.
"""

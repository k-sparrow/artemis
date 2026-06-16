# Record Manager & `lc_index` — how `upsertion_record` works (and our conventions)

> Reference for the LangChain indexing machinery (`langchain.indexes.index`/`aindex`
> + `SQLRecordManager`) that every upserter in this package sits on. Written so we
> don't have to re-derive it. A runnable, no-container demonstration lives at
> `.manual_tests/lc_aindex_record_manager_deepdive.py` — it indexes, re-indexes, and
> empty-cleans a SQLite-backed record manager and dumps the table after each step.

---

## TL;DR

- `lc_index(docs, record_manager, destination, …)` is a **diffing upsert**: it hashes each
  doc, asks the record manager "have I seen this hash before?", and only writes the
  *new/changed* ones to the destination (a `VectorStore` or a `DocumentIndex`), deleting
  stale ones per the `cleanup` mode.
- The **record manager is a bookkeeper, not a store.** It holds no content — just one row
  per unique chunk recording its hash, its tenant partition, its source group, and when it
  was last seen. The content lives in the destination, keyed by the same hash.
- Our two isolation/grouping principles map onto the table cleanly:
  **`namespace_id` → the `namespace` column** (tenant partition), **`obj_id` → the `group_id`
  column** (per-object cleanup pivot).

---

## 1. What `lc_index` + `RecordManager` do

```
lc_index(docs, record_manager, destination, *, cleanup, source_id_key, force_update, upsert_kwargs)
```

Per document it computes a content+metadata hash (`uid`). Then:
1. **exists?** `record_manager.exists([uids])` — already seen → **skip** (just refresh the
   timestamp); new/changed → **write** to the destination.
2. **write** — `VectorStore`: `add_documents(docs, ids=uids)`; `DocumentIndex`:
   `aupsert(docs)` where each `doc.id = uid`.
3. **record** — stamp every uid into the record manager with its `group_id` (source) and a
   fresh `updated_at`.
4. **cleanup** — per `cleanup` mode, delete stale rows (and their destination entries).

The record manager is what makes re-ingest cheap: unchanged chunks are *skipped*, not
re-embedded.

---

## 2. The `upsertion_record` table

One physical table, partitioned logically by `namespace`. **One row per unique chunk.**

| column | type | set from | meaning | we use it? |
|---|---|---|---|---|
| `uuid` | str, PK | ORM `default=uuid4()` (app-side) | surrogate row PK | **ignore** — pure plumbing |
| `key` | str | `hash(content ⊕ metadata)` (the "uid"/lc_hash) | the chunk's identity; **equals the destination key** (Qdrant point id / docstore key); what `list_keys()` returns; the dedup pivot | yes |
| `namespace` | str | **RM constructor** `f"{role}/{namespace_id}"` (fixed per RM instance) | tenant + store-role partition; every RM query filters `WHERE namespace = self.namespace` | yes → `namespace_id` |
| `group_id` | str, nullable | `doc.metadata[source_id_key]` (per-doc) | the source object; the cleanup/delete scoping pivot | yes → `obj_id` |
| `updated_at` | float | server time at write | freshness; the `scoped_full` survive/die gate | yes |

Constraints: **`UNIQUE(key, namespace)`** + a surrogate `uuid` PK. So the *real* identity is
the composite `(key, namespace)` — `key` alone is **not** table-unique: the same `key` can
appear once per `namespace` (two tenants indexing identical content → two rows, same `key`,
different `namespace`, different `uuid`). Dedup is therefore **per-tenant**.

---

## 3. How the `key` (uid) is derived

```python
NS = uuid.UUID(int=1984)
content_hash  = uuid5(NS, sha1(page_content))
metadata_hash = uuid5(NS, sha1(json.dumps(metadata, sort_keys=True)))
key (uid)     = uuid5(NS, sha1(content_hash + metadata_hash))
```

Both the **content and the full metadata** feed the key. Consequences:

| situation | result |
|---|---|
| identical content **and** identical metadata | same `key` → **deduped** to one row (this is the "5 docs in, 4 rows" effect) |
| identical content, **different** metadata (e.g. different `obj_id`) | **different** `key` → two rows, no collapse (why cross-object / cross-tenant identical content never merges) |
| any metadata field changes | **new** `key` → treated as a new/changed chunk (old row reaped on cleanup, new one written + re-embedded) |

> There is **no `key_encoder`** in our pinned `langchain-core` — the derivation above is
> hard-wired. You cannot reconfigure how the key is computed; you can only control what goes
> into `content`/`metadata`, and (for a `DocumentIndex`) override the *destination key*
> separately from the RM key (see §9).

---

## 4. Mapping: our isolation / grouping principles → the table

| our concept | what it is | → `upsertion_record` | granularity |
|---|---|---|---|
| **`namespace_id`** | tenant boundary (`private/{chat_id}` or `library/{project}`) | **`namespace`** column, as `f"{role}/{namespace_id}"` | coarse — per tenant, per store |
| **`obj_id`** | origin object, `uuid5(namespace_id, source_label)`, assigned by the **storage service** | **`group_id`** column (we set `source_id_key="obj_id"`) | fine — per object |
| chunk identity | `hash(content ⊕ metadata)` | **`key`** column | per chunk |
| **`group_id` (Epic 11)** | connector grouping (`connector_id` / `namespace_id`) | **NOT in the RM** — it's a Qdrant *payload* field; connector-group delete is a **fan-out of per-`obj_id` deletes** | above the RM |
| **`parent_id`** (was `doc_id`) | child→parent pointer for `MultiVectorRetriever` (`id_key`) | **NOT a RM column** — it rides *inside* `metadata`, so it's hashed *into* `key`; the RM never filters on it | retrieval linkage |

Two name traps to keep straight:
- **lc `group_id` = `obj_id`** (source pivot). The **Epic 11 `group_id` = connector** is a
  *different* thing and is not a record-manager concept.
- **`obj_id`** (storage-assigned, → `group_id`) vs **`parent_id`** (indexer-stamped, →
  `MultiVectorRetriever`). A child carries **both**.

---

## 5. The `namespace` role prefix — many ledgers, one table

`namespace` is fixed at RM construction (per request, from the tenant). We role-prefix it so
multiple ledgers coexist in one physical table, each still tenant-partitioned
([`ResourceConfig`](../../../backend/indexing/api/dependencies.py)):

| RM / store | `namespace` value | tracks |
|---|---|---|
| children (vectors) | `qdrant/{namespace_id}` | the embedded chunks in Qdrant |
| doc-store originals (table summaries) | `docstore/{namespace_id}:originals` | multi-vector originals |
| doc-store pages (Epic 19) | `docstore/{namespace_id}:pages` *(or `:parents`)* | parent pages |

Because each RM is built **per tenant** and every query filters on `namespace`, the
empty-list full cleanup (`aprocess([])`) wipes **exactly one tenant** — never global. The
per-tenant `namespace` is what makes that isolation load-bearing, not cosmetic.

---

## 6. Cleanup modes & the survive/die gate

| `cleanup` | what it deletes |
|---|---|
| `None` | nothing (pure add/skip) |
| `full` | every key in the `namespace` **not** present in this run |
| `scoped_full` | within the **source_ids seen in this batch**, every key **not** re-written this run (`group_id ∈ batch_source_ids AND updated_at < index_start`) |
| `scoped_full` + **empty docs** | degrades to **full** → wipes the whole `namespace` (this is how namespace-level delete works) |

The gate is two-part: **`group_id` ∈ batch's source set** *and* **`updated_at < index_start`**.
Re-submitted chunks get their `updated_at` refreshed (so they're *not* `< index_start` →
survive); dropped chunks keep the old timestamp → reaped. Source groups **not** in the batch
are never even considered (untouched).

Our config default is **`cleanup="scoped_full"`, `source_id_key="obj_id"`** — giving
*per-object replace-on-reingest* and *whole-namespace wipe* from one code path.

---

## 7. Lifecycle walkthrough (from the deep-dive script)

Index 5 chunks under two objects `A`/`B`, with one intra-`A` duplicate:

```
index() → num_added=4          # 5 in; the duplicate collapses (same key)
key=… group_id=A updated_at=t0
key=… group_id=A updated_at=t0
key=… group_id=B updated_at=t0
key=… group_id=B updated_at=t0
```

Re-index `A` with one chunk changed (`scoped_full`, `source_id_key=obj_id`):

```
index() → num_added=1, num_skipped=1, num_deleted=1
key=NEW… group_id=A updated_at=t1   # changed chunk → new key
key=…    group_id=A updated_at=t1   # unchanged → SAME key, timestamp REFRESHED → survives
key=…    group_id=B updated_at=t0   # B untouched (not in this batch's source set)
key=…    group_id=B updated_at=t0
```

Empty-list cleanup:

```
index([]) → num_deleted=4           # scoped_full + no sources ⇒ full namespace wipe
(0 rows)
```

---

## 8. Three-way key sync

By default the **same `key`** is the record-manager `key`, the **Qdrant point id**, *and* the
**doc-store key**. lc_index assumes this triple equality end to end:

- `_HashedDocument.from_document` ignores any incoming `doc.id` and sets `doc.id = uid`.
- `VectorStore` path passes `ids=uids`; `DocumentIndex` path passes **no** `ids` and our
  `StoreDocumentIndex` reads `item.id` (= uid).
- the RM stores that same uid as `key`; cleanup deletes the destination **by that uid**.

Break one corner (key the destination differently from the RM) and **deletion by uid stops
finding the object.** That's exactly the trade-off Epic 19 makes for pages — see §9.

---

## 9. Epic 19 parent-page convention — the deterministic-key trick

Pages want a **stable, deterministic** doc-store key `{namespace_id}/{obj_id}/p{page_no}` so a
child's `parent_id` never moves when a page's *content* changes (otherwise every child of an
edited page re-embeds — a cascade, because `parent_id` is hashed into the child's `key`).

But lc_index forces `destination_key == content-hash uid`. The resolution keeps lc_index's
free diffing **and** gets deterministic keys, by overriding only the *destination* key:

- **write:** pages go through lc_index, but `upsert_kwargs={"key": "page_key"}` tells a
  (~5-line) `StoreDocumentIndex.aupsert` to use `item.metadata["page_key"]` as the doc-store
  key instead of the uid. The RM still tracks the **content-hash uid**, so unchanged pages are
  still **skipped** (free diff); changed pages overwrite their deterministic key **in place**.
- **child link:** stamp `child.metadata["parent_id"] = page_key(...)` — the *same string* as
  the page's `page_key`.
- **delete:** the RM is uid-keyed but the store is deterministic-keyed, so lc_index's
  `adelete(uids)` **misses the page** (harmless no-op). Page deletion is therefore
  **prefix-based** on the byte store: shrink → `p{n>new_count}`, obj → `{ns}/{obj}/`,
  namespace → `{ns}/`.

### Two invariants this depends on

1. **One `page_key()` function.** `child.parent_id` and `page.page_key` must be byte-identical
   (`MultiVectorRetriever` does `mget(child.metadata[id_key])`; `create_kv_docstore`'s key
   encoder is identity). Compute both from a single shared function — drift = a **silent**
   empty-parent miss.
2. **lc_index clears the RM; we clear the store — never invert.** On delete, lc_index's
   `delete_keys(uids)` *does* clear the RM rows (RM is uid-keyed and has them); only the store
   `adelete` misses, so we **add** a prefix delete. Never prefix-delete the store while leaving
   RM rows behind — re-ingest would see "exists" in the RM, **skip the write**, and the page
   would be **missing**.

---

## 10. Gotchas / invariants (quick list)

- `key` is the chunk identity; `uuid` is throwaway row plumbing. We only ever reason about
  `key`, and filter on `namespace` + `group_id`.
- The metadata you stamp **before** `index()` is part of the `key` — so the normalizer
  (`namespace_id`, `group_id`, `parent_id`, …) changes the uid. Adding/renaming a metadata
  field re-hashes every chunk (fine pre-v1; re-index).
- `source_id_key` may be a **callable** `(Document) -> str`, not just a field name.
- Empty-list + `scoped_full` = full namespace wipe. Per-object delete uses
  `adelete_source(obj_id)` → `list_keys(group_ids=[obj_id])`; it never uses the empty list.
- Connector-group delete is a **fan-out of per-`obj_id` deletes** — a direct Qdrant
  `group_id`-filter delete would bypass the RM and desync it (stale rows → re-ingest skips →
  missing vectors).

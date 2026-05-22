"""Unit tests for RetrievalParams and build_qdrant_filter."""

from __future__ import annotations

import uuid

import pytest
from qdrant_client import models

from src.lib.core.retrieval.params import RetrievalParams, build_qdrant_filter


def _must_conditions(f: models.Filter) -> list[models.FieldCondition]:
    return f.must  # type: ignore[return-value]


class TestBuildQdrantFilter:
    def test_namespace_only_produces_one_condition(self) -> None:
        ns = uuid.uuid4()
        params = RetrievalParams(namespace_id=ns)

        f = build_qdrant_filter(params)

        conditions = _must_conditions(f)
        assert len(conditions) == 1
        assert conditions[0].key == "metadata.namespace_id"
        assert conditions[0].match.value == str(ns)

    def test_namespace_id_is_stringified(self) -> None:
        ns = uuid.uuid4()
        params = RetrievalParams(namespace_id=ns)

        f = build_qdrant_filter(params)

        assert _must_conditions(f)[0].match.value == str(ns)

    def test_string_namespace_id_accepted(self) -> None:
        ns_str = "some-namespace-string"
        params = RetrievalParams(namespace_id=ns_str)

        f = build_qdrant_filter(params)

        assert _must_conditions(f)[0].match.value == ns_str

    def test_group_id_adds_second_condition(self) -> None:
        ns = uuid.uuid4()
        gid = uuid.uuid4()
        params = RetrievalParams(namespace_id=ns, group_id=gid)

        f = build_qdrant_filter(params)

        conditions = _must_conditions(f)
        assert len(conditions) == 2
        keys = {c.key for c in conditions}
        assert "metadata.namespace_id" in keys
        assert "metadata.group_id" in keys

    def test_group_id_value_is_stringified(self) -> None:
        gid = uuid.uuid4()
        params = RetrievalParams(namespace_id=uuid.uuid4(), group_id=gid)

        f = build_qdrant_filter(params)

        group_cond = next(
            c for c in _must_conditions(f) if c.key == "metadata.group_id"
        )
        assert group_cond.match.value == str(gid)

    def test_doc_id_adds_obj_id_condition(self) -> None:
        ns = uuid.uuid4()
        doc = uuid.uuid4()
        params = RetrievalParams(namespace_id=ns, doc_id=doc)

        f = build_qdrant_filter(params)

        conditions = _must_conditions(f)
        assert len(conditions) == 2
        keys = {c.key for c in conditions}
        assert "metadata.obj_id" in keys

    def test_all_three_fields_produce_three_conditions(self) -> None:
        params = RetrievalParams(
            namespace_id=uuid.uuid4(),
            group_id=uuid.uuid4(),
            doc_id=uuid.uuid4(),
        )

        f = build_qdrant_filter(params)

        conditions = _must_conditions(f)
        assert len(conditions) == 3
        keys = {c.key for c in conditions}
        assert keys == {"metadata.namespace_id", "metadata.group_id", "metadata.obj_id"}

    def test_none_group_and_doc_id_omitted(self) -> None:
        params = RetrievalParams(namespace_id=uuid.uuid4(), group_id=None, doc_id=None)

        f = build_qdrant_filter(params)

        assert len(_must_conditions(f)) == 1

    def test_default_k_is_ten(self) -> None:
        params = RetrievalParams(namespace_id=uuid.uuid4())
        assert params.k == 10

    def test_params_are_frozen(self) -> None:
        params = RetrievalParams(namespace_id=uuid.uuid4())
        with pytest.raises((TypeError, AttributeError)):
            params.k = 99  # type: ignore[misc]

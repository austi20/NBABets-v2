from __future__ import annotations

from app.training.rotation import classify_archetype, normalize_role_vector


def test_normalize_role_vector_non_negative_and_sums_to_one() -> None:
    values = {"usage_share": 0.3, "assist_share": 0.2, "rebound_share": -1.0}
    normalized = normalize_role_vector(values)
    assert normalized["rebound_share"] == 0.0
    assert abs(sum(normalized.values()) - 1.0) < 1e-9


def test_classify_archetype_primary_creator() -> None:
    archetype = classify_archetype(
        position_group="G",
        usage_share=0.29,
        assist_share=0.31,
        rebound_share=0.11,
        three_point_rate=0.31,
        starter_score=0.9,
    )
    assert archetype == "primary_creator"


def test_classify_archetype_bench_depth() -> None:
    archetype = classify_archetype(
        position_group="F",
        usage_share=0.12,
        assist_share=0.08,
        rebound_share=0.12,
        three_point_rate=0.35,
        starter_score=0.1,
    )
    assert archetype == "bench_depth"

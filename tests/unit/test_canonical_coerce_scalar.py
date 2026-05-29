from app.providers.canonical_schema import _coerce_scalar


def test_coerce_scalar_preserves_bool() -> None:
    assert _coerce_scalar(True) is True
    assert _coerce_scalar(False) is False

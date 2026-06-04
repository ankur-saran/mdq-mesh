"""Unit tests for content_hash (C-5 determinism)."""

from mdq.utils.hashing import content_hash


def test_dict_order_does_not_matter() -> None:
    h1 = content_hash({"b": 2, "a": 1})
    h2 = content_hash({"a": 1, "b": 2})
    assert h1 == h2


def test_bytes_input() -> None:
    h = content_hash(b"hello")
    assert len(h) == 64  # SHA-256 hex digest


def test_str_and_bytes_equivalent() -> None:
    assert content_hash("hello") == content_hash(b"hello")


def test_different_data_different_hash() -> None:
    assert content_hash({"a": 1}) != content_hash({"a": 2})

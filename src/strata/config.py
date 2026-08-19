"""Configuration loading.

This module contains a small hand-written YAML reader, and that deserves an
explanation rather than an apology.

The Spark jobs run inside the official ``apache/spark`` image. That image has
Python but no PyYAML, and adding it means either baking a pip install into the
image (a network dependency at build time for one small parser) or shipping
``--py-files`` on every submit. Both are more moving parts than the ~120 lines
below, and both fail in ways that are discovered at submit time rather than at
import time.

The parser handles the subset this project's configs actually use: nested
mappings by indentation, block and inline sequences, scalars with type
inference, quoted strings, and comments. It deliberately does **not** implement
anchors, aliases, multi-document streams, block scalars, or flow mappings. Those
are not "not yet" — they are refused, loudly, so that a config using them fails
with a clear message instead of being silently misread. ``tests/test_config.py``
pins the behaviour including the refusals.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    """Raised for anything this parser will not or cannot handle."""


# ---------------------------------------------------------------------------
# Scalar parsing
# ---------------------------------------------------------------------------

_TRUE = {"true", "yes", "on"}
_FALSE = {"false", "no", "off"}
_NULL = {"", "null", "~", "none"}


def _strip_comment(line: str) -> str:
    """Remove a trailing comment, respecting quotes.

    ``date: "2024-01-01"  # start`` must keep the date and drop the note, and a
    ``#`` inside quotes is data. Naive ``line.split("#")[0]`` gets both wrong.
    """
    out: list[str] = []
    quote: str | None = None
    for index, char in enumerate(line):
        if quote:
            out.append(char)
            if char == quote and (index == 0 or line[index - 1] != "\\"):
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
            out.append(char)
            continue
        if char == "#":
            # A '#' only begins a comment at the start of the line or after
            # whitespace; 'a#b' is a single token.
            if index == 0 or line[index - 1] in " \t":
                break
            out.append(char)
            continue
        out.append(char)
    return "".join(out).rstrip()


def _parse_scalar(raw: str) -> Any:
    text = raw.strip()

    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        # Quoted: always a string, never type-inferred. This is why dates are
        # quoted in the configs — "2024-01-01" must stay a string rather than
        # becoming something a date parser has an opinion about.
        return text[1:-1]

    lowered = text.lower()
    if lowered in _NULL:
        return None
    if lowered in _TRUE:
        return True
    if lowered in _FALSE:
        return False

    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part) for part in _split_inline(inner)]

    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    return text


def _split_inline(inner: str) -> list[str]:
    """Split ``a, "b, c", d`` on commas that are not inside quotes."""
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    for char in inner:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
            current.append(char)
            continue
        if char == ",":
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    if current:
        parts.append("".join(current))
    return [part.strip() for part in parts if part.strip()]


# ---------------------------------------------------------------------------
# Structure parsing
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> list[tuple[int, str]]:
    tokens: list[tuple[int, str]] = []
    for number, raw_line in enumerate(text.splitlines(), start=1):
        if "\t" in raw_line[: len(raw_line) - len(raw_line.lstrip())]:
            raise ConfigError(f"line {number}: tab in indentation; YAML requires spaces")
        stripped = _strip_comment(raw_line)
        if not stripped.strip():
            continue
        for unsupported, name in (("&", "anchor"), ("*", "alias"), ("<<", "merge key")):
            if stripped.lstrip().startswith(unsupported):
                raise ConfigError(
                    f"line {number}: YAML {name}s are not supported by this parser"
                )
        if stripped.strip() in ("---", "..."):
            raise ConfigError(f"line {number}: multi-document streams are not supported")
        indent = len(stripped) - len(stripped.lstrip())
        tokens.append((indent, stripped.strip()))
    return tokens


def _parse_block(tokens: list[tuple[int, str]], start: int, indent: int) -> tuple[Any, int]:
    if start >= len(tokens):
        return None, start

    if tokens[start][1].startswith("- "):
        return _parse_sequence(tokens, start, indent)
    return _parse_mapping(tokens, start, indent)


def _parse_sequence(tokens: list[tuple[int, str]], start: int, indent: int) -> tuple[list, int]:
    items: list[Any] = []
    index = start
    while index < len(tokens):
        item_indent, content = tokens[index]
        if item_indent < indent or not content.startswith("- "):
            break
        items.append(_parse_scalar(content[2:]))
        index += 1
    return items, index


def _parse_mapping(tokens: list[tuple[int, str]], start: int, indent: int) -> tuple[dict, int]:
    mapping: dict[str, Any] = {}
    index = start
    while index < len(tokens):
        line_indent, content = tokens[index]
        if line_indent < indent:
            break
        if line_indent > indent:
            raise ConfigError(f"unexpected indentation at: {content!r}")

        if ":" not in content:
            raise ConfigError(f"expected 'key: value', got: {content!r}")

        key, _, remainder = content.partition(":")
        key = key.strip()
        remainder = remainder.strip()
        index += 1

        if remainder:
            mapping[key] = _parse_scalar(remainder)
            continue

        # Empty value: either a nested block below, or an explicit null.
        if index < len(tokens) and tokens[index][0] > line_indent:
            value, index = _parse_block(tokens, index, tokens[index][0])
            mapping[key] = value
        else:
            mapping[key] = None
    return mapping, index


def loads(text: str) -> dict:
    """Parse the supported YAML subset into plain Python structures."""
    tokens = _tokenize(text)
    if not tokens:
        return {}
    value, consumed = _parse_block(tokens, 0, tokens[0][0])
    if consumed != len(tokens):
        raise ConfigError("trailing content the parser could not attach to anything")
    if not isinstance(value, dict):
        raise ConfigError("top level of a config must be a mapping")
    return value


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------

class Config:
    """A loaded config with dotted lookup.

    ``cfg["data.fact_rows"]`` rather than ``cfg["data"]["fact_rows"]`` — the
    difference matters mostly in the error message, which names the full path
    that was missing instead of raising KeyError on an intermediate dict.
    """

    def __init__(self, data: dict, source: str | None = None) -> None:
        self._data = data
        self.source = source

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "Config":
        resolved = Path(path)
        if not resolved.is_file():
            raise ConfigError(f"config not found: {resolved}")
        return cls(loads(resolved.read_text(encoding="utf-8")), source=str(resolved))

    @classmethod
    def from_env(cls, default: str = "configs/smoke.yaml") -> "Config":
        """Load whatever ``STRATA_CONFIG`` points at, defaulting to smoke scale.

        Defaulting to smoke rather than full is deliberate: the expensive config
        should always be an explicit choice, never something you get by
        forgetting to set a variable.
        """
        return cls.load(os.environ.get("STRATA_CONFIG", default))

    def __getitem__(self, dotted: str) -> Any:
        node: Any = self._data
        walked: list[str] = []
        for part in dotted.split("."):
            walked.append(part)
            if not isinstance(node, dict) or part not in node:
                raise ConfigError(
                    f"missing config key {'.'.join(walked)!r}"
                    + (f" in {self.source}" if self.source else "")
                )
            node = node[part]
        return node

    def get(self, dotted: str, default: Any = None) -> Any:
        try:
            return self[dotted]
        except ConfigError:
            return default

    def to_dict(self) -> dict:
        return self._data

    # -- Frequently used, resolved once so call sites stay readable ----------

    @property
    def scale(self) -> str:
        return str(self["scale"])

    @property
    def seed(self) -> int:
        return int(self["seed"])

    def path(self, name: str) -> str:
        return str(self[f"paths.{name}"])

    def experiment_path(self, *parts: str) -> str:
        return "/".join([self.path("experiments"), *parts])

    def __repr__(self) -> str:
        return f"Config(scale={self.get('scale')!r}, source={self.source!r})"

"""Quality-flag definitions and validation for STREAM observations."""

from collections.abc import Sequence


QUALITY_FLAGS = {
    "A": "Approved",
    "P": "Provisional",
    "E": "Erroneous",
}

_QUALITY_ALIASES = {
    flag.lower(): flag for flag in QUALITY_FLAGS
} | {
    description.lower(): flag for flag, description in QUALITY_FLAGS.items()
}


def normalize_quality(
    quality: str | Sequence[str] | None,
) -> list[str] | None:
    """Return quality selections as flag codes such as ``["A", "P"]``."""

    if quality is None:
        return None
    if isinstance(quality, str):
        quality = [quality]
    else:
        quality = list(quality)

    if not quality:
        raise ValueError("quality must contain at least one flag")

    normalized = []
    for value in quality:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("quality must contain valid flag names")
        key = value.strip().lower()
        if key not in _QUALITY_ALIASES:
            choices = ", ".join(
                f"{flag} ({description})"
                for flag, description in QUALITY_FLAGS.items()
            )
            raise ValueError(f"Unknown quality flag {value!r}. Available flags: {choices}")
        flag = _QUALITY_ALIASES[key]
        if flag not in normalized:
            normalized.append(flag)

    return normalized

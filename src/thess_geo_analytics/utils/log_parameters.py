from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Mapping, Optional


def log_parameters(
    entrypoint_name: str,
    params: Any,
    docs: Optional[Mapping[str, str]] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> None:
    """
    Log a nicely formatted parameter block for an entrypoint.

    - params can be a dataclass instance or a plain dict-like.
    - docs maps param_name -> human-readable description.
    - extra lets you add contextual fields like mode, region, aoi_id.
    """
    docs = docs or {}
    extra = extra or {}

    # Normalize params to a dict
    if is_dataclass(params):
        data = asdict(params)
    elif isinstance(params, Mapping):
        data = dict(params)
    else:
        data = {"value": repr(params)}

    print()
    print("_____________________________________________________________")
    print(f"[ENTRYPOINT] {entrypoint_name}")
    print("[PARAMETERS]")

    # Extra context first (mode, region, aoi, etc.)
    for key, value in extra.items():
        print(f"  {key} = {value}")

    for key, value in data.items():
        meaning = docs.get(key, "")
        if meaning:
            print(f"  {key} = {value}    ({meaning})")
        else:
            print(f"  {key} = {value}")

    print("-" * 60)
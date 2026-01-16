# workers/sheet_contract.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional


def _s(v: Any) -> str:
    return str(v or "").strip()


@dataclass(frozen=True)
class SheetContract:
    """
    Shared contract utilities.
    - Pure functions only (stateless)
    - Worker-specific required columns (avoid false failures)
    """

    @staticmethod
    def assert_columns_present(headers: List[str], required: Iterable[str], tab_name: str) -> None:
        hs = {h.strip() for h in headers if h and h.strip()}
        missing = [c for c in required if c not in hs]
        if missing:
            raise RuntimeError(
                f"{tab_name} schema invalid. Missing required columns: {missing}. "
                f"Present={sorted(hs)}"
            )

    @staticmethod
    def parse_iso_utc(value: Any) -> Optional[datetime]:
        s = _s(value)
        if not s:
            return None
        try:
            # Accept Z or +00:00
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dtv = datetime.fromisoformat(s)
            if dtv.tzinfo is None:
                dtv = dtv.replace(tzinfo=timezone.utc)
            return dtv.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def is_older_than_seconds(value: Any, min_age_seconds: int) -> bool:
        dtv = SheetContract.parse_iso_utc(value)
        if not dtv:
            return False
        now = datetime.now(timezone.utc)
        age = (now - dtv).total_seconds()
        return age >= float(min_age_seconds)

    @staticmethod
    def now_iso_utc_z() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

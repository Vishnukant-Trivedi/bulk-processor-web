from __future__ import annotations

import hashlib
import re
from typing import Dict, Optional, Tuple


_space_re = re.compile(r"\s+")
_phone_re = re.compile(r"\D+")


def normalize_text(value: str) -> str:
    return _space_re.sub(" ", (value or "").strip()).casefold()


def normalize_phone(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = _phone_re.sub("", value)
    return cleaned or None


def build_dedupe_key(name: str, address: str, phone: Optional[str]) -> str:
    payload = "|".join([normalize_text(name), normalize_text(address), normalize_phone(phone) or ""])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def row_to_payload(row: Dict[str, str], batch_id: str) -> Dict[str, str]:
    payload = {
        "name": (row.get("name") or "").strip(),
        "address": (row.get("address") or "").strip(),
        "creation_batch_id": batch_id,
    }
    phone = (row.get("phone") or "").strip()
    if phone:
        payload["phone"] = phone
    return payload

def normalize_row(row: Dict[str, str]) -> Tuple[str, str, Optional[str], str]:
    name = (row.get("name") or "").strip()
    address = (row.get("address") or "").strip()
    phone = (row.get("phone") or "").strip() or None
    dedupe_key = build_dedupe_key(name, address, phone)
    return name, address, phone, dedupe_key

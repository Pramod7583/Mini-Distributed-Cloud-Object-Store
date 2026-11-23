"""
controller/app/integrity/checksum.py
SHA-256 checksum helpers.
"""
import hashlib


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify(data: bytes, expected: str) -> bool:
    return sha256(data) == expected

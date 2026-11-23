"""
worker/app/integrity/checksum.py
SHA-256 helpers for the worker.
"""
import hashlib


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

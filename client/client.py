#!/usr/bin/env python3
"""
client/client.py — Interactive CLI client for the Mini Distributed Cloud Object Store.

Usage:
    python -m client.cli upload <file_path> [--metadata '{"key":"value"}']
    python -m client.cli download <object_id> [--out <output_path>]
    python -m client.cli metadata <object_id>
    python -m client.cli update-metadata <object_id> '{"key":"value"}'
    python -m client.cli delete <object_id>
    python -m client.cli list
    python -m client.cli health
"""

import json
import os
import sys

import requests

BASE_URL = os.environ.get("CONTROLLER_URL", "http://localhost:8000")


def upload(file_path: str, metadata: str | None = None) -> dict:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f)}
        data = {}
        if metadata:
            data["metadata"] = metadata
        resp = requests.post(f"{BASE_URL}/api/v1/objects", files=files, data=data)
    resp.raise_for_status()
    return resp.json()


def download(object_id: str, out_path: str | None = None) -> str:
    resp = requests.get(f"{BASE_URL}/api/v1/objects/{object_id}", stream=True)
    resp.raise_for_status()

    # Determine output filename
    dest = out_path
    if not dest:
        cd = resp.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            dest = cd.split("filename=")[-1].strip('"')
        else:
            dest = object_id
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(8192):
            f.write(chunk)
    return dest


def get_metadata(object_id: str) -> dict:
    resp = requests.get(f"{BASE_URL}/api/v1/objects/{object_id}/metadata")
    resp.raise_for_status()
    return resp.json()


def update_metadata(object_id: str, patch: dict) -> dict:
    resp = requests.patch(
        f"{BASE_URL}/api/v1/objects/{object_id}/metadata",
        json=patch,
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def delete_object(object_id: str) -> None:
    resp = requests.delete(f"{BASE_URL}/api/v1/objects/{object_id}")
    resp.raise_for_status()


def list_objects() -> dict:
    resp = requests.get(f"{BASE_URL}/api/v1/objects")
    resp.raise_for_status()
    return resp.json()


def health() -> dict:
    resp = requests.get(f"{BASE_URL}/health")
    resp.raise_for_status()
    return resp.json()

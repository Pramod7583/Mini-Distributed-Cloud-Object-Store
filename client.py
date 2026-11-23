#!/usr/bin/env python3
"""
client.py — Interactive CLI client for the Mini Distributed Cloud Object Store.

Usage:
    python client.py upload <file_path> [--metadata '{"key":"value"}']
    python client.py download <object_id> [--out <output_path>]
    python client.py metadata <object_id>
    python client.py update-metadata <object_id> '{"key":"value"}'
    python client.py delete <object_id>
    python client.py list
    python client.py health
"""

import argparse
import json
import os
import sys
import requests

BASE_URL = os.environ.get("CONTROLLER_URL", "http://localhost:8000")


def upload(args):
    path = args.file
    if not os.path.exists(path):
        print(f"ERROR: File not found: {path}")
        sys.exit(1)

    with open(path, "rb") as f:
        files = {"file": (os.path.basename(path), f)}
        data = {}
        if args.metadata:
            data["metadata"] = args.metadata

        print(f"Uploading {path} ...")
        resp = requests.post(f"{BASE_URL}/api/v1/objects", files=files, data=data)

    if resp.status_code == 201:
        result = resp.json()
        print("✅ Upload successful!")
        print(json.dumps(result, indent=2))
    else:
        print(f"❌ Upload failed [{resp.status_code}]: {resp.text}")
        sys.exit(1)


def download(args):
    object_id = args.object_id
    print(f"Downloading object {object_id} ...")
    resp = requests.get(f"{BASE_URL}/api/v1/objects/{object_id}", stream=True)

    if resp.status_code == 200:
        out_path = args.out or object_id
        # Try to get filename from Content-Disposition
        cd = resp.headers.get("Content-Disposition", "")
        if "filename=" in cd:
            out_path = cd.split("filename=")[-1].strip('"')
        if args.out:
            out_path = args.out

        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        size = os.path.getsize(out_path)
        print(f"✅ Downloaded to '{out_path}' ({size} bytes)")
    else:
        print(f"❌ Download failed [{resp.status_code}]: {resp.text}")
        sys.exit(1)


def metadata(args):
    resp = requests.get(f"{BASE_URL}/api/v1/objects/{args.object_id}/metadata")
    if resp.status_code == 200:
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"❌ [{resp.status_code}]: {resp.text}")
        sys.exit(1)


def update_metadata(args):
    try:
        body = json.loads(args.data)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        sys.exit(1)

    resp = requests.patch(
        f"{BASE_URL}/api/v1/objects/{args.object_id}/metadata",
        json=body,
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code == 200:
        print("✅ Metadata updated:")
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"❌ [{resp.status_code}]: {resp.text}")
        sys.exit(1)


def delete(args):
    resp = requests.delete(f"{BASE_URL}/api/v1/objects/{args.object_id}")
    if resp.status_code == 204:
        print(f"✅ Object {args.object_id} deleted.")
    else:
        print(f"❌ [{resp.status_code}]: {resp.text}")
        sys.exit(1)


def list_objects(args):
    resp = requests.get(f"{BASE_URL}/api/v1/objects")
    if resp.status_code == 200:
        result = resp.json()
        print(f"Total objects: {result['total']}")
        for obj in result["objects"]:
            print(
                f"  [{obj['object_id']}] {obj['name']} "
                f"({obj['size']} bytes, {obj['content_type']})"
            )
    else:
        print(f"❌ [{resp.status_code}]: {resp.text}")
        sys.exit(1)


def health(args):
    resp = requests.get(f"{BASE_URL}/health")
    if resp.status_code == 200:
        print(json.dumps(resp.json(), indent=2))
    else:
        print(f"❌ [{resp.status_code}]: {resp.text}")
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────────────
# CLI parsing
# ──────────────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="Cloud Object Store Client")
sub = parser.add_subparsers(dest="command")

p_upload = sub.add_parser("upload", help="Upload a file")
p_upload.add_argument("file", help="Path to file to upload")
p_upload.add_argument("--metadata", help='JSON custom metadata, e.g. \'{"tag":"demo"}\'', default=None)

p_download = sub.add_parser("download", help="Download an object")
p_download.add_argument("object_id")
p_download.add_argument("--out", help="Output path", default=None)

p_meta = sub.add_parser("metadata", help="Get object metadata")
p_meta.add_argument("object_id")

p_update = sub.add_parser("update-metadata", help="Update custom metadata")
p_update.add_argument("object_id")
p_update.add_argument("data", help='JSON object, e.g. \'{"status":"reviewed"}\'')

p_delete = sub.add_parser("delete", help="Delete an object")
p_delete.add_argument("object_id")

sub.add_parser("list", help="List all objects")
sub.add_parser("health", help="Show system health")

args = parser.parse_args()

commands = {
    "upload": upload,
    "download": download,
    "metadata": metadata,
    "update-metadata": update_metadata,
    "delete": delete,
    "list": list_objects,
    "health": health,
}

if args.command in commands:
    commands[args.command](args)
else:
    parser.print_help()

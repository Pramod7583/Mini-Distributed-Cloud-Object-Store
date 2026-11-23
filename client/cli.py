#!/usr/bin/env python3
"""
client/cli.py — Argument-parsing CLI wrapper around client.py.

Run from the project root:
    python -m client.cli <command> [args]
"""

import argparse
import json
import sys

from client.client import (
    delete_object,
    download,
    get_metadata,
    health,
    list_objects,
    update_metadata,
    upload,
)


def cmd_upload(args):
    result = upload(args.file, args.metadata)
    print("✅ Upload successful!")
    print(json.dumps(result, indent=2))


def cmd_download(args):
    dest = download(args.object_id, args.out)
    print(f"✅ Downloaded to '{dest}'")


def cmd_metadata(args):
    print(json.dumps(get_metadata(args.object_id), indent=2))


def cmd_update_metadata(args):
    try:
        patch = json.loads(args.data)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        sys.exit(1)
    result = update_metadata(args.object_id, patch)
    print("✅ Metadata updated:")
    print(json.dumps(result, indent=2))


def cmd_delete(args):
    delete_object(args.object_id)
    print(f"✅ Object {args.object_id} deleted.")


def cmd_list(args):
    result = list_objects()
    print(f"Total objects: {result['total']}")
    for obj in result["objects"]:
        print(f"  [{obj['object_id']}] {obj['name']} ({obj['size']} bytes, {obj['content_type']})")


def cmd_health(args):
    print(json.dumps(health(), indent=2))


parser = argparse.ArgumentParser(description="Cloud Object Store CLI")
sub = parser.add_subparsers(dest="command")

p = sub.add_parser("upload", help="Upload a file")
p.add_argument("file")
p.add_argument("--metadata", default=None)

p = sub.add_parser("download", help="Download an object")
p.add_argument("object_id")
p.add_argument("--out", default=None)

p = sub.add_parser("metadata", help="Get object metadata")
p.add_argument("object_id")

p = sub.add_parser("update-metadata", help="Update custom metadata")
p.add_argument("object_id")
p.add_argument("data")

p = sub.add_parser("delete", help="Delete an object")
p.add_argument("object_id")

sub.add_parser("list", help="List all objects")
sub.add_parser("health", help="Show system health")

_commands = {
    "upload": cmd_upload,
    "download": cmd_download,
    "metadata": cmd_metadata,
    "update-metadata": cmd_update_metadata,
    "delete": cmd_delete,
    "list": cmd_list,
    "health": cmd_health,
}

if __name__ == "__main__":
    args = parser.parse_args()
    if args.command in _commands:
        _commands[args.command](args)
    else:
        parser.print_help()

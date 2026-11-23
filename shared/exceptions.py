"""
shared/exceptions.py
Custom exceptions used across the system.
"""


class ObjectNotFound(Exception):
    pass


class ChunkNotFound(Exception):
    pass


class InsufficientWorkers(Exception):
    pass


class ReplicationFailed(Exception):
    pass


class ChecksumMismatch(Exception):
    pass

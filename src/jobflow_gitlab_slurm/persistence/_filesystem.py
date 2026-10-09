"""Internal descriptor and durability mechanics, without domain decisions.

Callers own identity validation, lock acquisition, publication sequencing, and
error translation. These functions neither retry nor clean up retained evidence.
"""

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def regular_file_descriptor(path: Path) -> Iterator[int]:
    """Open a no-follow, nonblocking regular file and always close its descriptor.

    The established non-regular-file diagnostic is retained verbatim so that
    moving this primitive does not change contextual error handling.
    """
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("definition artifact must be a regular file")
        yield descriptor
    finally:
        os.close(descriptor)


def sync_file(path: Path) -> None:
    """Flush one existing regular file; the caller owns ordering and ownership."""
    with regular_file_descriptor(path) as descriptor:
        os.fsync(descriptor)


def sync_directory(path: Path) -> None:
    """Flush a no-follow directory descriptor and close it even on failure."""
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

"""Named observations with deliberately different evidence coverage."""

import os
import stat


def descendant_metadata_and_bytes(root):
    """Observe descendants' modes, mtimes and regular bytes, excluding root/inodes."""
    result = {}
    for path in root.rglob("*"):
        metadata = path.lstat()
        result[str(path.relative_to(root))] = (
            metadata.st_mode,
            metadata.st_mtime_ns,
            path.read_bytes() if stat.S_ISREG(metadata.st_mode) else None,
        )
    return result


def tree_identity_and_content(root):
    """Observe root and descendants, retaining inodes and literal symlink targets."""
    observations = []
    for path in sorted((root, *root.rglob("*"))):
        metadata = path.lstat()
        if stat.S_ISREG(metadata.st_mode):
            data = path.read_bytes()
        elif stat.S_ISLNK(metadata.st_mode):
            data = os.readlink(path)
        else:
            data = None
        observations.append(
            (
                path.relative_to(root).as_posix(),
                metadata.st_mode,
                metadata.st_ino,
                metadata.st_mtime_ns,
                data,
            )
        )
    return tuple(observations)

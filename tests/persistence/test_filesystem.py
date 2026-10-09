"""Descriptor flags, lifetime, and synchronization of shared primitives."""

import os

import pytest

from jobflow_gitlab_slurm.persistence import _filesystem as filesystem


@pytest.mark.parametrize("body_failure", [False, True])
def test_regular_descriptor_flags_and_lifetime(tmp_path, monkeypatch, body_failure):
    path = tmp_path / "payload"
    path.write_bytes(b"opaque")
    original = os.open
    calls = []

    def record_open(path, flags):
        calls.append(flags)
        return original(path, flags)

    monkeypatch.setattr(filesystem.os, "open", record_open)
    if body_failure:
        with (
            pytest.raises(RuntimeError, match="body failure"),
            filesystem.regular_file_descriptor(path) as descriptor,
        ):
            assert os.read(descriptor, 6) == b"opaque"
            raise RuntimeError("body failure")
    else:
        with filesystem.regular_file_descriptor(path) as descriptor:
            assert os.read(descriptor, 6) == b"opaque"
    assert calls == [os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK]
    with pytest.raises(OSError):
        os.fstat(descriptor)


@pytest.mark.parametrize("kind", ["directory", "fifo", "symlink"])
def test_nonregular_and_symlink_entries_are_rejected(tmp_path, kind):
    path = tmp_path / "unsafe"
    if kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        target = tmp_path / "payload"
        target.write_bytes(b"opaque")
        path.symlink_to(target)
    expected = OSError if kind == "symlink" else ValueError
    with pytest.raises(expected), filesystem.regular_file_descriptor(path):
        pytest.fail("unsafe entries must never yield a descriptor")


@pytest.mark.parametrize("directory", [False, True])
def test_sync_failure_closes_descriptor_and_preserves_entry(
    tmp_path, monkeypatch, directory
):
    path = tmp_path / "entry"
    if directory:
        path.mkdir()
    else:
        path.write_bytes(b"opaque")
    before = path.stat().st_ino
    descriptors = []

    def fail_sync(descriptor):
        descriptors.append(descriptor)
        raise OSError("flush failure")

    monkeypatch.setattr(filesystem.os, "fsync", fail_sync)
    operation = filesystem.sync_directory if directory else filesystem.sync_file
    with pytest.raises(OSError, match="flush failure"):
        operation(path)
    assert len(descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(descriptors[0])
    assert path.stat().st_ino == before
    if not directory:
        assert path.read_bytes() == b"opaque"

"""Content digests of the input data files read by the Steps.

A Step's `config_hash` needs to change whenever one of the files it reads changes, and *not*
change otherwise. Identifying such a file by its path does neither reliably: the path is
absolute, so a `main_directory` synchronized between two machines never reuses its cache; it
says nothing about the file's content, so editing a file in place can go unnoticed; and it
changes when a file is merely moved, re-running steps for nothing.

Files are therefore identified by a digest of their content. Hashing gigabytes on every run
would be far too slow, so digests are cached and recomputed only when a file's size or
modification time changes.

Note. The cache is nothing but a memo of a pure function of the path, the size and the
modification time of a file. Losing it, or discarding a corrupted one, can only ever cost time,
never correctness; this is why none of the error handling below reports an error, and why two
pipelines sharing a `main_directory` need no locking.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat as stat_module
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import humanize
from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Iterator

# Size of the digests, in bytes. These are cache keys for local files, not a security boundary,
# so a truncated BLAKE2b is more than enough and keeps the cache file small.
DIGEST_SIZE = 16
# Digest stored for a parameter which is defined but whose path does not exist. It must not be
# `None`, which is the value of a parameter which is not defined at all: creating a previously
# missing file has to invalidate the steps reading it.
MISSING_DIGEST = "missing"
# Seeds the hasher of a directory, so that an empty directory and an empty file do not get the
# same digest. The digest of a file is deliberately left unseeded, so that it stays reproducible
# with `b2sum -l 128 <file>`.
DIRECTORY_TAG = b"\x00dir\x00"
# Files at least this large take long enough to hash that the first run should say so.
LOG_SIZE_THRESHOLD = 64 * 1024 * 1024


def _new_hasher():
    """Returns the hash object used for every digest.

    BLAKE2b is used rather than MD5 / SHA-1 / SHA-256 because it is part of the standard library
    but is *not* backed by OpenSSL (so it keeps working when the system enforces FIPS mode, where
    `hashlib.md5()` raises), because its speed does not depend on whether the CPU implements the
    SHA extensions (so a digest cache behaves the same on every machine), and because it supports
    truncated digests natively.
    """
    return hashlib.blake2b(digest_size=DIGEST_SIZE)


class DigestCache:
    """Content digests of the input data files, cached on their size and modification time.

    The cache is stored as a JSON file, mapping the absolute path of a file to its size, its
    modification time and its digest. A digest is recomputed only when the size or the
    modification time of the file changed, so a run which changes nothing only pays one `stat`
    call per file.
    """

    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self._entries = self._read()
        # Paths consulted during this run. Only these are written back, so that the cache file
        # does not grow forever when the content of a digested directory changes over time.
        self._used: set[str] = set()
        # Whether a digest was computed since the cache was read, i.e. whether `save` has anything
        # to write.
        self._dirty = False

    def _read(self) -> dict[str, Any]:
        """Reads the cache file, returning an empty cache if it cannot be read.

        A corrupted or outdated cache only costs time (every digest is recomputed), so it is
        discarded rather than reported.
        """
        if not self.cache_path.is_file():
            return dict()
        try:
            with open(self.cache_path, encoding="utf-8") as f:
                entries = json.load(f)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            logger.debug(f"Discarding unreadable digest cache `{self.cache_path}`")
            return dict()
        if not isinstance(entries, dict):
            return dict()
        return entries

    def save(self):
        """Writes the cache file, if any digest was computed since it was read.

        The file is written to a temporary file which then replaces the cache file, so that a run
        interrupted while saving cannot leave a truncated JSON file behind, and so that two
        pipelines sharing a `main_directory` cannot interleave their writes. Should they run
        concurrently, the last one to save simply wins and the other's digests are recomputed on
        its next run.
        """
        if not self._dirty:
            return
        entries = {k: v for k, v in self._entries.items() if k in self._used}
        tmp_fd, tmp_name = tempfile.mkstemp(dir=self.cache_path.parent, suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(entries, f)
            os.replace(tmp_name, self.cache_path)
        except OSError as e:
            logger.debug(f"Could not save the digest cache `{self.cache_path}`: {e}")
            Path(tmp_name).unlink(missing_ok=True)
            return
        self._dirty = False

    def digest_value(self, value: Any) -> Any:
        """Returns `value` with its paths replaced by the digest of the file they point to.

        A `Path` is replaced by its digest, a list is mapped element by element, and any other
        value is returned unchanged.

        Note. The dispatch is done on the *value*, not on the type of the Parameter which produced
        it, so that a `ListParameter(inner=PathType(...))` (e.g. `gtfs.files`) is handled like a
        `PathParameter`, without having to enumerate the Parameter classes which happen to produce
        a path.
        """
        if isinstance(value, Path):
            return self.digest_path(value)
        if isinstance(value, list):
            return [self.digest_value(v) for v in value]
        return value

    def digest_path(self, path: Path) -> str:
        """Returns the digest of a file or of a directory, or `MISSING_DIGEST` if it is neither.

        Whether the path is a file or a directory is decided here rather than from the Parameter
        which produced it, because `survey.mobisurvstd.path` can be either one, depending on
        `survey.mobisurvstd.bulk`.
        """
        try:
            stat = os.stat(path)
        except OSError:
            return MISSING_DIGEST
        if stat_module.S_ISDIR(stat.st_mode):
            return self._digest_directory(path)
        return self._digest_file(path, stat)

    def _cached_digest(self, path: Path, stat: os.stat_result) -> str | None:
        """Returns the cached digest of `path`, or `None` if it has to be computed."""
        entry = self._entries.get(str(path))
        if (
            isinstance(entry, dict)
            and entry.get("size") == stat.st_size
            and entry.get("mtime_ns") == stat.st_mtime_ns
            and isinstance(entry.get("digest"), str)
        ):
            return entry["digest"]
        return None

    def _digest_file(self, path: Path, stat: os.stat_result) -> str:
        key = str(path)
        self._used.add(key)
        cached = self._cached_digest(path, stat)
        if cached is not None:
            return cached
        if stat.st_size >= LOG_SIZE_THRESHOLD:
            logger.info(f"Hashing `{path}` ({humanize.naturalsize(stat.st_size)})")
        else:
            logger.debug(f"Hashing `{path}`")
        try:
            with open(path, "rb") as f:
                digest = hashlib.file_digest(f, _new_hasher).hexdigest()
        except OSError as e:
            logger.warning(f"Could not read `{path}`: {e}")
            return MISSING_DIGEST
        self._entries[key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "digest": digest}
        self._dirty = True
        return digest

    def _digest_directory(self, path: Path) -> str:
        """Returns a digest of every file of a directory, recursively.

        The name of each file is hashed alongside its content, so that renaming, adding or removing
        a file changes the digest. Names are relative to the directory, so that moving the whole
        directory does not.

        Note. The composite digest of a directory is deliberately *not* cached: there is no size
        and modification time which would validly key it, since the modification time of a
        directory does not change when one of the files it contains is modified. The directory is
        therefore walked on every run — which only costs one `stat` call per file, the digest of
        each individual file being cached as usual.
        """
        files = sorted(self._iter_files(path), key=lambda item: item[0])
        # Announce the whole directory at once rather than file by file, and only count what
        # actually has to be hashed, so that a warm run stays silent.
        pending = sum(
            stat.st_size for _, file, stat in files if self._cached_digest(file, stat) is None
        )
        if pending >= LOG_SIZE_THRESHOLD:
            logger.info(f"Hashing `{path}` ({len(files)} files, {humanize.naturalsize(pending)})")
        hasher = _new_hasher()
        hasher.update(DIRECTORY_TAG)
        for name, file, stat in files:
            hasher.update(name.encode())
            hasher.update(b"\0")
            hasher.update(self._digest_file(file, stat).encode())
            hasher.update(b"\0")
        return hasher.hexdigest()

    def _iter_files(self, root: Path) -> Iterator[tuple[str, Path, os.stat_result]]:
        """Yields `(name relative to root, path, stat)` for every file under `root`, recursively.

        Walks with `os.scandir` rather than `Path.rglob` so that telling a file from a directory
        does not cost an extra `stat` call: this can be thousands of calls for the IGN databases
        or an Eqasim output directory, which may sit on a network mount.
        """
        directories = [root]
        while directories:
            current = directories.pop()
            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                directories.append(Path(entry.path))
                            elif entry.is_file():
                                file = Path(entry.path)
                                yield file.relative_to(root).as_posix(), file, entry.stat()
                        except OSError:
                            # The entry vanished, or is not readable: treated as absent.
                            continue
            except OSError as e:
                logger.warning(f"Could not read directory `{current}`: {e}")

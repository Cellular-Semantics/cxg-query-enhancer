"""
Tests for the on-disk census terms cache used by _get_census_terms.

Covers:
- Cache location (env override, XDG_CACHE_HOME, default ~/.cache)
- Version aliases such as "latest" are resolved before keying the cache
- Cache hits, misses on a new release, corrupted files
- Atomic, best-effort writes
"""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa

from cxg_query_enhancer.enhancer import (
    CACHE_DIR_ENV_VAR,
    _get_cache_dir,
    _get_census_terms,
)

COLUMN = "cell_type_ontology_term_id"


def fake_open_soma(values):
    """Build a mock for cellxgene_census.open_soma serving one obs column in two chunks."""
    obs = MagicMock()
    obs.keys.return_value = [COLUMN]
    half = len(values) // 2
    obs.read.return_value = [
        pa.table({COLUMN: values[:half]}),
        pa.table({COLUMN: values[half:]}),
    ]
    census = {"census_data": {"homo_sapiens": MagicMock(obs=obs)}}
    open_soma = MagicMock()
    open_soma.return_value.__enter__.return_value = census
    return open_soma


class TestCacheDir(unittest.TestCase):
    def test_env_override(self):
        with patch.dict(os.environ, {CACHE_DIR_ENV_VAR: "/tmp/custom"}):
            self.assertEqual(_get_cache_dir(), "/tmp/custom")

    def test_xdg_cache_home(self):
        env = {"XDG_CACHE_HOME": "/tmp/xdg"}
        with patch.dict(os.environ, env):
            os.environ.pop(CACHE_DIR_ENV_VAR, None)
            self.assertEqual(_get_cache_dir(), "/tmp/xdg/cxg_query_enhancer")

    def test_default_is_independent_of_cwd(self):
        with patch.dict(os.environ, {}):
            os.environ.pop(CACHE_DIR_ENV_VAR, None)
            os.environ.pop("XDG_CACHE_HOME", None)
            self.assertEqual(
                _get_cache_dir(),
                os.path.join(os.path.expanduser("~"), ".cache", "cxg_query_enhancer"),
            )


class TestCensusTermsCache(unittest.TestCase):
    def setUp(self):
        _get_census_terms.cache_clear()
        self.tmp = tempfile.TemporaryDirectory()
        env = patch.dict(os.environ, {CACHE_DIR_ENV_VAR: self.tmp.name})
        env.start()
        self.addCleanup(env.stop)
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(_get_census_terms.cache_clear)

    def cache_path(self, build):
        return os.path.join(self.tmp.name, f"{build}_homosapiens_{COLUMN}.json")

    def get_terms(self, open_soma, build="2025-11-17", version="latest"):
        _get_census_terms.cache_clear()  # exercise the disk cache, not lru_cache
        with patch(
            "cxg_query_enhancer.enhancer.cellxgene_census.get_census_version_description",
            return_value={"release_build": build},
        ), patch("cxg_query_enhancer.enhancer.cellxgene_census.open_soma", open_soma):
            return _get_census_terms(version, "homo_sapiens", COLUMN)

    def test_latest_is_resolved_and_cached_as_json(self):
        open_soma = fake_open_soma(["CL:2", "CL:1", "unknown", None, "CL:1"])

        terms = self.get_terms(open_soma)

        self.assertEqual(terms, {"CL:1", "CL:2"})
        open_soma.assert_called_once_with(census_version="2025-11-17")
        with open(self.cache_path("2025-11-17")) as f:
            self.assertEqual(json.load(f), ["CL:1", "CL:2"])
        # No temp files left behind by the atomic write
        self.assertEqual(
            os.listdir(self.tmp.name), [f"2025-11-17_homosapiens_{COLUMN}.json"]
        )

    def test_cache_hit_skips_census(self):
        self.get_terms(fake_open_soma(["CL:1"]))
        open_soma = fake_open_soma(["CL:999"])

        terms = self.get_terms(open_soma)

        self.assertEqual(terms, {"CL:1"})
        open_soma.assert_not_called()

    def test_new_release_is_a_cache_miss(self):
        self.get_terms(fake_open_soma(["CL:1"]), build="2025-11-17")
        open_soma = fake_open_soma(["CL:1", "CL:2"])

        terms = self.get_terms(open_soma, build="2026-01-01")

        self.assertEqual(terms, {"CL:1", "CL:2"})
        open_soma.assert_called_once_with(census_version="2026-01-01")
        self.assertTrue(os.path.exists(self.cache_path("2026-01-01")))

    def test_corrupted_cache_file_is_refetched(self):
        with open(self.cache_path("2025-11-17"), "w") as f:
            f.write("{not json")
        open_soma = fake_open_soma(["CL:1"])

        terms = self.get_terms(open_soma)

        self.assertEqual(terms, {"CL:1"})
        open_soma.assert_called_once()
        with open(self.cache_path("2025-11-17")) as f:
            self.assertEqual(json.load(f), ["CL:1"])

    def test_legacy_pickle_cache_is_ignored(self):
        legacy = os.path.join(self.tmp.name, f"latest_homosapiens_{COLUMN}.pkl")
        with open(legacy, "wb") as f:
            f.write(b"not loaded")
        open_soma = fake_open_soma(["CL:1"])

        self.assertEqual(self.get_terms(open_soma), {"CL:1"})
        open_soma.assert_called_once()

    def test_cache_write_failure_still_returns_terms(self):
        with patch(
            "cxg_query_enhancer.enhancer.os.replace",
            side_effect=OSError("read-only filesystem"),
        ):
            terms = self.get_terms(fake_open_soma(["CL:1"]))

        self.assertEqual(terms, {"CL:1"})
        # The temp file was cleaned up and no cache file was written
        self.assertEqual(os.listdir(self.tmp.name), [])


if __name__ == "__main__":
    unittest.main()

"""Unit tests for the metadata server placement record: choosing the storage nodes to pin
it to, and the record's round trip through the run directory."""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import UTC, datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import sbtest  # noqa: E402,F401  (registers the bundled plugins)
from sbtest.adapters import ArchiveEvidence  # noqa: E402
from sbtest.components import mds_placement  # noqa: E402
from sbtest.core import MdsInstance, MdsPlacement  # noqa: E402

LABEL = "io.simplyblock.storagenodeset"


class StorageSelector(unittest.TestCase):
    def test_the_node_set_most_storage_nodes_carry_is_chosen(self):
        nodes = {"master-1": {}, "worker-1": {LABEL: "set-a"}, "worker-2": {LABEL: "set-a"},
                 "worker-3": {LABEL: "set-b"}}
        self.assertEqual(mds_placement.storage_selector(nodes, LABEL), {LABEL: "set-a"})
        self.assertEqual(mds_placement.storage_nodes(nodes, LABEL), ["worker-1", "worker-2", "worker-3"])

    def test_a_cluster_without_storage_nodes_has_no_selector(self):
        self.assertIsNone(mds_placement.storage_selector({"master-1": {}}, LABEL))


class Record(unittest.TestCase):
    def test_the_record_round_trips_through_the_run_directory(self):
        t0 = datetime(2026, 10, 14, 13, 0, tzinfo=UTC)
        record = MdsPlacement(pinned=True, storage_nodes=["worker-1"],
                              instances=[MdsInstance(pod="mds-0", node="worker-1", ip="10.0.0.1",
                                                     first=t0, last=t0)])
        with tempfile.TemporaryDirectory() as d:
            mds_placement.write_record(os.path.join(d, mds_placement.RECORD), record)
            self.assertEqual(ArchiveEvidence(d).mds_placement(), record)


if __name__ == "__main__":
    unittest.main()

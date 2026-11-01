package com.example.refdata.client;

import com.example.refdata.model.RefDataStore;
import com.example.refdata.protocol.v1.SnapshotInfo;

/**
 * The result of a load: the immutable, indexed store, the server's description of the snapshot it came from, and
 * what the load cost.
 */
public record LoadedSnapshot(RefDataStore store, SnapshotInfo info, LoadStats stats) {
}

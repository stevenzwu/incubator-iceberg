/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.source.enumerator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.FlinkSplitGenerator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.parquet.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This callable runs in a separate I/O thread pool.
 * It is critical it doesn't modify any enumerator state.
 * For safety, we moved the logic to a separate class.
 *
 * This also make it a littler easier for unit testing,
 * as the main complexity is in this planner class.
 */
public class ContinuousSplitPlannerImpl implements ContinuousSplitPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousSplitPlannerImpl.class);

  private final Table table;
  private final ContinuousEnumeratorConfig config;
  private final ScanContext scanContext;

  public ContinuousSplitPlannerImpl(Table table, ContinuousEnumeratorConfig config, ScanContext scanContext) {
    this.table = table;
    this.config = config;
    this.scanContext = scanContext;
    validate();
  }

  private void validate() {
    Preconditions.checkArgument(scanContext.snapshotId() == null,
        "Can't set snapshotId in ScanContext for continuous enumerator");
    Preconditions.checkArgument(scanContext.asOfTimestamp() == null,
        "Can't set asOfTimestamp in ScanContext for continuous enumerator");
    Preconditions.checkArgument(scanContext.startSnapshotId() == null,
        "Can't set startSnapshotId in ScanContext for continuous enumerator");
    Preconditions.checkArgument(scanContext.endSnapshotId() == null,
        "Can't set endSnapshotId in ScanContext for continuous enumerator");
  }

  @Override
  public SplitPlanningResult planSplits(Optional<Long> lastEnumeratedSnapshotId) {

    table.refresh();
    if (lastEnumeratedSnapshotId.isPresent()) {
      // incremental discovery mode
      final long endSnapshotId = table.currentSnapshot().snapshotId();
      final long startSnapshotId = lastEnumeratedSnapshotId.get();
      final List<IcebergSourceSplit> splits;
      if (endSnapshotId == lastEnumeratedSnapshotId.get()) {
        LOG.info("Current table snapshot is already enumerated: {}",
             lastEnumeratedSnapshotId.get());
        splits = Collections.emptyList();
      } else {
        final ScanContext incrementalScan = scanContext
            .copyWithAppendsBetween(startSnapshotId, endSnapshotId);
        splits = FlinkSplitGenerator.planIcebergSourceSplits(table, incrementalScan);
        LOG.info("Discovered {} splits from increment scan: startSnapshotId = {}, endSnapshotId = {}",
            splits.size(), lastEnumeratedSnapshotId.get(), endSnapshotId);
      }
      return new SplitPlanningResult(splits, endSnapshotId);
    } else {
      // first time
      final long startSnapshotId = getStartSnapshotId(table, config);
      LOG.info("get startSnapshotId {} based on starting strategy {}",
          startSnapshotId, config.startingStrategy());
      final List<IcebergSourceSplit> splits;
      if (config.startingStrategy() ==
          ContinuousEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL) {
        // do a full table scan first
        splits = FlinkSplitGenerator
            .planIcebergSourceSplits(table, scanContext);
        LOG.info("Discovered {} splits from initial full table scan with snapshotId {}",
            splits.size(), startSnapshotId);
      } else {
        splits = Collections.emptyList();
      }
      return new SplitPlanningResult(splits, startSnapshotId);
    }
  }

  @VisibleForTesting
  static long getStartSnapshotId(
      final Table table,
      final ContinuousEnumeratorConfig contEnumConfig) {
    final List<HistoryEntry> historyEntries = table.history();
    final long startSnapshotId;
    switch (contEnumConfig.startingStrategy()) {
      case TABLE_SCAN_THEN_INCREMENTAL:
      case LATEST_SNAPSHOT:
        startSnapshotId = historyEntries.get(historyEntries.size() - 1).snapshotId();
        break;
      case EARLIEST_SNAPSHOT:
        startSnapshotId = historyEntries.get(0).snapshotId();
        break;
      case SPECIFIC_START_SNAPSHOT_ID:
        startSnapshotId = contEnumConfig.startSnapshotId();
        break;
      case SPECIFIC_START_SNAPSHOT_TIMESTAMP:
        Optional<Long> opt = findSnapshotIdByTimestamp(
            contEnumConfig.startSnapshotTimeMs(), table.history());
        if (opt.isPresent()) {
          startSnapshotId = opt.get();
        } else {
          throw new IllegalArgumentException(
              "Failed to find a start snapshot in history using timestamp: " +
                  contEnumConfig.startSnapshotTimeMs());
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown starting strategy: " +
            contEnumConfig.startingStrategy());
    }
    return startSnapshotId;
  }

  @VisibleForTesting
  static Optional<Long> findSnapshotIdByTimestamp(long timestamp, List<HistoryEntry> historyEntries) {
    Long lastSnapshotId = null;
    for (HistoryEntry logEntry : historyEntries) {
      if (logEntry.timestampMillis() <= timestamp) {
        lastSnapshotId = logEntry.snapshotId();
      }
    }
    return Optional.ofNullable(lastSnapshotId);
  }

}

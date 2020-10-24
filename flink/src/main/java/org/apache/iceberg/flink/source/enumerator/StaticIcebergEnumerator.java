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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSplitGenerator;
import org.apache.iceberg.flink.source.IcebergSourceEvents;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-time split enumeration at the beginning
 */
public class StaticIcebergEnumerator<
    SplitAssignerStateT extends SplitAssignerState> implements
    SplitEnumerator<IcebergSourceSplit, IcebergEnumState> {
  private static final Logger LOG = LoggerFactory.getLogger(StaticIcebergEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumContext;
  private final TableLoader tableLoader;
  private final ScanContext scanContext;
  private final SplitAssigner<SplitAssignerStateT> assigner;

  private volatile Optional<Long> lastEnumeratedSnapshotId;
  private volatile Table table;

  public StaticIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      TableLoader tableLoader,
      ScanContext scanContext,
      SplitAssigner<SplitAssignerStateT> assigner,
      @Nullable IcebergEnumState<SplitAssignerStateT> enumState) {
    this.enumContext = enumContext;
    this.tableLoader = tableLoader;
    this.scanContext = scanContext;
    this.assigner = assigner;
    if (enumState != null) {
      this.lastEnumeratedSnapshotId = enumState.lastEnumeratedSnapshotId();
    } else {
      this.lastEnumeratedSnapshotId = Optional.empty();
    }
  }

  @Override
  public void start() {
    // start the splits discovery if not completed yet
    if (!lastEnumeratedSnapshotId.isPresent()) {
      table = loadTable();
      enumContext.callAsync(
          this::discoverSplits,
          this::processDiscoveredSplits);
    }
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof IcebergSourceEvents.SplitRequestEvent) {
      final IcebergSourceEvents.SplitRequestEvent splitRequestEvent =
          (IcebergSourceEvents.SplitRequestEvent) sourceEvent;
      assigner.onSplitsCompletion(subtaskId, splitRequestEvent.finishedSplitIds());
      assignNextEvents(subtaskId);
    } else {
      LOG.error("Received unrecognized event: {}", sourceEvent);
    }
  }

  @Override
  public void addSplitsBack(List<IcebergSourceSplit> splits, int subtaskId) {
    LOG.info("Add {} assigned splits back to the pool for failed subtask {}: {}",
        splits.size(), subtaskId, splits);
    assigner.addSplits(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Added reader {}", subtaskId);
    // reader requests for split upon start
    // nothing for enumerator to do upon registration

    // TODO: remove this code along with
    // the change in IcebergSourceReader.start()
    // when the ordering bug fix in master branch is ported to 1.11.3.
    assignNextEvents(subtaskId);
  }

  @Override
  public IcebergEnumState snapshotState() throws Exception {
    return new IcebergEnumState(lastEnumeratedSnapshotId, assigner.splitAssignerState());
  }

  @Override
  public void close() throws IOException {
  }

  private Table loadTable() {
    LOG.info("Load table");
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      return loader.loadTable();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close table loader", e);
    }
  }

  private List<IcebergSourceSplit> discoverSplits() {
    return FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
  }

  private void processDiscoveredSplits(List<IcebergSourceSplit> splits, Throwable error) {
    if (error == null) {
      assigner.addSplits(splits);
      assigner.onNoMoreSplits();
      lastEnumeratedSnapshotId = Optional.of(getEnumeratedSnapshotId());
    } else {
      // TODO: should we do the split enumeration in the source construction?
      // pros: split discovery failure will cause the job submission
      //       fail immediately (instead of retries internally)
      // cons: that would cause the job submission/creation slow.
      LOG.error("Failed to enumerate splits", error);
      // schedule a retry
      enumContext.callAsync(
          this::discoverSplits,
          this::processDiscoveredSplits);
    }
  }

  /**
   * Right now, it simply return the table's current snapshot id.
   * Here, we just need a value (doesn't matter what it is)
   * to indicate that enumeration is completed.
   */
  private long getEnumeratedSnapshotId() {
    return table.currentSnapshot().snapshotId();
  }

  private void assignNextEvents(int subtask) {
    LOG.info("Subtask {} is requesting a new split", subtask);
    assigner.getNext(subtask).thenAccept(split -> {
      if (split != null) {
        SplitsAssignment assignment = new SplitsAssignment(
            ImmutableMap.of(subtask, Arrays.asList(split)));
        enumContext.assignSplits(assignment);
        LOG.info("Assigned split to subtask {}: {}", subtask, split);
      } else {
        enumContext.sendEventToSourceReader(subtask, new NoMoreSplitsEvent());
        LOG.info("No more splits available for subtask {}", subtask);
      }
    });
  }
}

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
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.SourceEvents;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.planner.SplitPlanner;
import org.apache.iceberg.flink.source.planner.SplitsPlanningResult;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-time split enumeration at the beginning
 */
public class IcebergSplitEnumerator implements
    SplitEnumerator<IcebergSourceSplit, IcebergEnumState> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSplitEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumContext;
  private final Table table;
  private final ScanContext scanContext;
  private final ContinuousEnumSettings contEnumSettings;
  private final SplitPlanner planner;
  private final SplitAssigner assigner;

  public IcebergSplitEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      Table table,
      ScanContext scanContext,
      @Nullable ContinuousEnumSettings contEnumSettings,
      SplitPlanner planner,
      SplitAssigner assigner) {
    this.enumContext = enumContext;
    this.table = table;
    this.scanContext = scanContext;
    this.contEnumSettings = contEnumSettings;
    this.planner = planner;
    this.assigner = assigner;
  }

  @Override
  public void start() {
    if (contEnumSettings != null) {
      LOG.info("Starting the IcebergSplitEnumerator for continuous mode: {}",
          contEnumSettings.getDiscoveryInterval());
      enumContext.callAsync(
          () -> planner.planSplits(table, scanContext),
          this::handleSplitsDiscovery,
          0,
          contEnumSettings.getDiscoveryInterval().toMillis());
    } else {
      LOG.info("Starting the IcebergSplitEnumerator for static mode");
      enumContext.callAsync(
          () -> planner.planSplits(table, scanContext),
          this::handleSplitsDiscovery);
    }
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof SourceEvents.SplitRequestEvent) {
      final SourceEvents.SplitRequestEvent splitRequestEvent =
          (SourceEvents.SplitRequestEvent) sourceEvent;
      assigner.onSplitsCompletion(subtaskId, splitRequestEvent.finishedSplitIds());
      assignNextEvents(subtaskId);
    } else {
      LOG.error("Received unrecognized event: {}", sourceEvent);
    }
  }

  @Override
  public void addSplitsBack(List<IcebergSourceSplit> splits, int subtaskId) {
    LOG.info("Add {} splits back to the pool for failed subtask {}: {}",
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
    return new IcebergEnumState(planner.state(), assigner.state());
  }

  @Override
  public void close() throws IOException {
    // no resources to close
  }

  private void handleSplitsDiscovery(SplitsPlanningResult result, Throwable error) {
    if (error != null) {
      LOG.error("Failed to discover splits", error);
    } else {
      assigner.addSplits(result.splits());
      if (result.noMoreSplits()) {
        assigner.noMoreSplits();
      }
    }
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

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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.iceberg.flink.source.SourceEvents;
import org.apache.iceberg.flink.source.assigner.Assigner;
import org.apache.iceberg.flink.source.planner.SplitsPlanningResult;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-time split enumeration at the beginning
 */
public abstract class AbstractEnumerator<EnumStateT extends AbstractEnumState> implements
    SplitEnumerator<IcebergSourceSplit, EnumStateT> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumContext;
  private final Assigner assigner;

  public AbstractEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      Assigner assigner) {
    this.enumContext = enumContext;
    this.assigner = assigner;
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
  public void close() throws IOException {
    // no resources to close
  }

  protected void handleSplitsDiscovery(SplitsPlanningResult result, Throwable error) {
    if (error != null) {
      LOG.error("Failed to discover splits", error);
    } else {
      assigner.addSplits(result.splits());
      if (result.noMoreSplits()) {
        assigner.onNoMoreSplits();
      }
    }
  }

  private void assignNextEvents(int subtask) {
    LOG.info("Subtask {} is requesting a new split", subtask);
    Optional<SplitsAssignment<IcebergSourceSplit>> assignment = assigner.getAssignment(subtask);
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

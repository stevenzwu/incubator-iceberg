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

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.parquet.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousIcebergEnumerator extends AbstractIcebergEnumerator {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousIcebergEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumContext;
  private final ScanContext scanContext;
  private final SplitAssigner assigner;
  private final ContinuousEnumConfig contEnumConfig;
  private final Table table;
  private final ContinuousSplitPlanner splitPlanner;

  /**
   * snapshotId for the last enumerated snapshot.
   * next incremental enumeration should based off this as starting position.
   */
  private volatile Optional<Long> lastEnumeratedSnapshotId;

  public ContinuousIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      TableLoader tableLoader,
      ScanContext scanContext,
      SplitAssigner assigner,
      @Nullable IcebergEnumState enumState,
      ContinuousEnumConfig contEnumConfig) {
    super(enumContext, assigner);

    this.enumContext = enumContext;
    this.scanContext = scanContext;
    this.assigner = assigner;
    this.contEnumConfig = contEnumConfig;
    validate();

    this.table = loadTable(tableLoader);
    this.splitPlanner = new ContinuousSplitPlanner();
    if (enumState != null) {
      this.lastEnumeratedSnapshotId = enumState.lastEnumeratedSnapshotId();
    } else {
      this.lastEnumeratedSnapshotId = Optional.empty();
    }
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
  public void start() {
    enumContext.callAsync(
        this::discoverSplits,
        this::processDiscoveredSplits,
        0L,
        contEnumConfig.discoveryInterval().toMillis());
  }

  @Override
  protected boolean shouldWaitForMoreSplits() {
    return true;
  }

  @Override
  public IcebergEnumState snapshotState() throws Exception {
    return new IcebergEnumState(lastEnumeratedSnapshotId, assigner.snapshotState());
  }

  private SplitPlanningResult discoverSplits() {
    return splitPlanner.planSplits(table, contEnumConfig,
        scanContext, lastEnumeratedSnapshotId);
  }

  private void processDiscoveredSplits(SplitPlanningResult result, Throwable error) {
    if (error == null) {
      assigner.onDiscoveredSplits(result.splits());
      lastEnumeratedSnapshotId = Optional.of(result.lastEnumeratedSnapshotId());
    } else {
      LOG.error("Failed to enumerate splits", error);
    }
  }

}

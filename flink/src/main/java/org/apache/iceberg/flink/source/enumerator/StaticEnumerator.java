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

import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.assigner.Assigner;
import org.apache.iceberg.flink.source.planner.SplitPlanner;
import org.apache.iceberg.flink.source.planner.SplitsPlanningResult;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticEnumerator extends AbstractEnumerator<StaticEnumState> {
  private static final Logger LOG = LoggerFactory.getLogger(StaticEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumContext;
  private final Table table;
  private final ScanContext scanContext;

  public StaticEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      Table table,
      ScanContext scanContext,
      @Nullable ContinuousEnumSettings contEnumSettings,
      SplitPlanner planner,
      Assigner assigner) {
    super(enumContext, assigner);
    this.enumContext = enumContext;
    this.table = table;
    this.scanContext = scanContext;
  }

  @Override
  public void start() {
    LOG.info("Starting the IcebergSplitEnumerator for static mode");
    enumContext.callAsync(
        () -> planSplits(),
        this::handleSplitsDiscovery);
  }

  private SplitsPlanningResult planSplits() {
    return null;
  }

  @Override
  public StaticEnumState snapshotState() throws Exception {
    return null;
  }
}

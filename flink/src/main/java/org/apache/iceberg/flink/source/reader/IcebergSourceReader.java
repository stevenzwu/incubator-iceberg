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

package org.apache.iceberg.flink.source.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.flink.source.IcebergSourceEvents;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.MutableIcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSourceReader<T> extends
    SingleThreadMultiplexSourceReaderBase<RecordAndPosition<T>, T, IcebergSourceSplit, MutableIcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceReader.class);

  public IcebergSourceReader(
      SourceReaderContext context,
      BulkFormat<T, IcebergSourceSplit> readerFormat) {
    super(
        () -> new IcebergSourceSplitReader<>(context.getConfiguration(), readerFormat),
        new IcebergSourceRecordEmitter(),
        context.getConfiguration(),
        context);
  }

  /**
   * Reader requests on split during start,
   * which means that reader can be momentarily idle
   * while waiting for the new assigned splits.
   * We need to do more testing to verify
   * if it is a concern or not in practice.
   *
   * If we want to avoid empty backlog of splits,
   * we can send to request split events.
   */
  @Override
  public void start() {
   requestSplit(Collections.emptyList());
  }

  @Override
  protected void onSplitFinished(Map<String, MutableIcebergSourceSplit> finishedSplitIds) {
    if (!finishedSplitIds.isEmpty()) {
      requestSplit(new ArrayList<>(finishedSplitIds.keySet()));
    }
  }

  @Override
  protected MutableIcebergSourceSplit initializedState(IcebergSourceSplit split) {
    return MutableIcebergSourceSplit.fromSplit(split);
  }

  @Override
  protected IcebergSourceSplit toSplitType(
      String splitId,
      MutableIcebergSourceSplit splitState) {
    return IcebergSourceSplit.fromSplitState(splitState);
  }

  private void requestSplit(Collection<String> finishedSplitIds) {
    context.sendSourceEventToCoordinator(new IcebergSourceEvents.SplitRequestEvent(finishedSplitIds));
  }
}

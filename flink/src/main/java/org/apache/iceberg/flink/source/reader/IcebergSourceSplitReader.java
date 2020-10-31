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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import javax.annotation.Nullable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.util.BulkFormat;
import org.apache.iceberg.flink.source.util.CheckpointedPosition;
import org.apache.iceberg.flink.source.util.RecordAndPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSourceSplitReader<T> implements SplitReader<T, IcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceSplitReader.class);

  private final Configuration config;
  private final BulkFormat<T, IcebergSourceSplit> readerFactory;
  private final Queue<IcebergSourceSplit> splits;

  @Nullable
  private BulkFormat.Reader<T> currentReader;
  @Nullable
  private String currentSplitId;

  public IcebergSourceSplitReader(
      Configuration config,
      BulkFormat<T, IcebergSourceSplit> readerFactory) {
    this.config = config;
    this.readerFactory = readerFactory;
    this.splits = new ArrayDeque<>();
  }

  @Override
  public RecordsWithSplitIds<T> fetch() {
    return null;
  }

  // TODO: replace the above fetch once upgrade to 1.11.3
  public org.apache.iceberg.flink.source.util.RecordsWithSplitIds<RecordAndPosition<T>> fetchNew() throws IOException {
    checkSplitOrStartNext();
    final BulkFormat.RecordIterator<T> nextBatch = currentReader.readBatch();
    return nextBatch == null ? finishSplit() : FileRecords.forRecords(currentSplitId, nextBatch);
  }

  @Override
  public void handleSplitsChanges(Queue<SplitsChange<IcebergSourceSplit>> splitsChanges) {
    while (!splitsChanges.isEmpty()) {
      SplitsChange<IcebergSourceSplit> splitChange = splitsChanges.poll();
      if (splitChange instanceof SplitsAddition) {
        LOG.debug("Handling split change: {}", splitChange);
        splits.addAll(splitChange.splits());
      } else {
        throw new UnsupportedOperationException(
            splitChange.getClass().getName() + " is not supported");
      }
    }
  }

  @Override
  public void wakeUp() {
  }

  private void checkSplitOrStartNext() throws IOException {
    if (currentReader != null) {
      return;
    }
    final IcebergSourceSplit nextSplit = splits.poll();
    if (nextSplit == null) {
      throw new IOException("No split remaining");
    }
    currentSplitId = nextSplit.splitId();
    final CheckpointedPosition position = nextSplit.checkpointedPosition();
    currentReader = position != null ? readerFactory.restoreReader(config, nextSplit)
        : readerFactory.createReader(config, nextSplit);
  }

  private FileRecords<T> finishSplit() throws IOException {
    if (currentReader != null) {
      currentReader.close();
      currentReader = null;
    }
    final FileRecords<T> finishRecords = FileRecords.finishedSplit(currentSplitId);
    currentSplitId = null;
    return finishRecords;
  }
}

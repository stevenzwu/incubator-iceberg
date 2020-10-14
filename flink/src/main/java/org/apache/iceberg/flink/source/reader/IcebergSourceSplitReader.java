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
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import javax.annotation.Nullable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.iceberg.flink.TableInfo;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.IcebergSourceOptions;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSourceSplitReader<T> implements SplitReader<RecordAndPosition<T>, IcebergSourceSplit> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceSplitReader.class);

  private final int batchSize;
  private final DataIteratorFactory<T> iteratorFactory;
  private final TableInfo tableInfo;
  private final ScanContext scanContext;
  private final Queue<IcebergSourceSplit> splits;

  // cache the splitId to avoid repeated computation
  @Nullable private String currentSplitId;
  @Nullable private DataIterator<T> currentIterator;
  private long currentPosition = 0L;

  private volatile boolean wakeUp = false;

  public IcebergSourceSplitReader(
      Configuration config,
      DataIteratorFactory<T> iteratorFactory,
      TableInfo tableInfo,
      ScanContext scanContext) {
    this.batchSize = config.get(IcebergSourceOptions.READER_FETCH_BATCH_SIZE);
    this.iteratorFactory = iteratorFactory;
    this.tableInfo = tableInfo;
    this.scanContext = scanContext;
    this.splits = new ArrayDeque<>();
  }

  @Override
  public RecordsWithSplitIds<RecordAndPosition<T>> fetch() throws InterruptedException {
    setupIterator();

    final RecordsBySplits<RecordAndPosition<T>> result = new RecordsBySplits<>();
    if (currentIterator == null) {
      LOG.debug("iterator is null. skip fetch");
      return result;
    }

    final List<RecordAndPosition<T>> fetchedRecords = new ArrayList<>(batchSize);
    while (!wakeUp &&
        currentIterator.hasNext() &&
        fetchedRecords.size() < batchSize) {
      fetchedRecords.add(new RecordAndPosition(currentIterator.next(), currentPosition));
      currentPosition++;
    }
    result.addAll(currentSplitId, fetchedRecords);

    if (!currentIterator.hasNext()) {
      result.addFinishedSplit(currentSplitId);
      try {
        currentIterator.close();
      } catch (IOException e) {
        LOG.error("Failed to close iterator for split: {}", currentSplitId, e);
      } finally {
        currentSplitId = null;
        currentIterator = null;
        currentPosition = 0L;
      }
    }

    wakeUp = false;
    return result;
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
    wakeUp = true;
  }

  private void setupIterator() throws InterruptedException {
    if (currentIterator != null) {
      return;
    }
    final IcebergSourceSplit split = splits.poll();
    if (split == null) {
      LOG.debug("no split available");
      return;
    }

    final String newSplitId = split.splitId();
    final DataIterator<T> newIterator = iteratorFactory
        .createIterator(split, tableInfo, scanContext, false);
    long newPosition = 0L;

    // skip already processed records
    while (!wakeUp && newIterator.hasNext() &&
        newPosition < split.startingPosition()) {
      newIterator.next();
      newPosition++;
    }

    if (wakeUp) {
      wakeUp = false;
      return;
    }

    if (currentPosition < split.startingPosition()) {
      throw new IllegalStateException(String.format(
          "Split doesn't contain. This indicates some bug in reader code. " +
              "startingPosition = %d, splitId = %s",
          split.startingPosition(), split.splitId()));
    }

    currentSplitId = newSplitId;
    currentIterator = newIterator;
    currentPosition = newPosition;
  }
}

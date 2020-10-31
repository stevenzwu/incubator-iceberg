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

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.iceberg.flink.source.util.BulkFormat;
import org.apache.iceberg.flink.source.util.RecordAndPosition;
import org.apache.iceberg.flink.source.util.RecordsWithSplitIds;

/**
 * Copied from Flink's master branch
 */
public class FileRecords<T> implements RecordsWithSplitIds<RecordAndPosition<T>> {

  @Nullable
  private String splitId;

  @Nullable
  private BulkFormat.RecordIterator<T> recordsForSplitCurrent;

  @Nullable
  private final BulkFormat.RecordIterator<T> recordsForSplit;

  private final Set<String> finishedSplits;

  private FileRecords(
      @Nullable String splitId,
      @Nullable BulkFormat.RecordIterator<T> recordsForSplit,
      Set<String> finishedSplits) {

    this.splitId = splitId;
    this.recordsForSplit = recordsForSplit;
    this.finishedSplits = finishedSplits;
  }

  @Nullable
  @Override
  public String nextSplit() {
    // move the split one (from current value to null)
    final String nextSplit = this.splitId;
    this.splitId = null;

    // move the iterator, from null to value (if first move) or to null (if second move)
    this.recordsForSplitCurrent = nextSplit != null ? this.recordsForSplit : null;

    return nextSplit;
  }

  @Nullable
  @Override
  public RecordAndPosition<T> nextRecordFromSplit() {
    final BulkFormat.RecordIterator<T> recordsIter = this.recordsForSplitCurrent;
    if (recordsIter != null) {
      return recordsIter.next();
    } else {
      throw new IllegalStateException();
    }
  }

  @Override
  public void recycle() {
    if (recordsForSplit != null) {
      recordsForSplit.releaseBatch();
    }
  }

  @Override
  public Set<String> finishedSplits() {
    return finishedSplits;
  }

  // ------------------------------------------------------------------------

  public static <T> FileRecords<T> forRecords(
      final String splitId,
      final BulkFormat.RecordIterator<T> recordsForSplit) {
    return new FileRecords<>(splitId, recordsForSplit, Collections.emptySet());
  }

  public static <T> FileRecords<T> finishedSplit(String splitId) {
    return new FileRecords<>(null, null, Collections.singleton(splitId));
  }

}

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

package org.apache.iceberg.flink.source.util;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.util.Preconditions;

/**
 * Copied from Flink master branch for now
 */
public class CheckpointedPosition implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Constant for the offset, reflecting that the position does not contain any offset information.
   * It is used in positions that are defined only by a number of records to skip.
   */
  public static final long NO_OFFSET = -1L;

  private final long offset;
  private final long recordsAfterOffset;

  /**
   * Creates a new CheckpointedPosition for given offset and records-to-skip.
   *
   * @param offset The offset that the reader will seek to when restored from this checkpoint.
   * @param recordsAfterOffset The records to skip after the offset.
   */
  public CheckpointedPosition(long offset, long recordsAfterOffset) {
    Preconditions.checkArgument(offset >= -1, "offset must be >= 0 or NO_OFFSET");
    Preconditions.checkArgument(recordsAfterOffset >= 0, "recordsAfterOffset must be >= 0");
    this.offset = offset;
    this.recordsAfterOffset = recordsAfterOffset;
  }

  /**
   * Gets the offset that the reader will seek to when restored from this checkpoint.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Gets the records to skip after the offset.
   */
  public long getRecordsAfterOffset() {
    return recordsAfterOffset;
  }

  // ------------------------------------------------------------------------

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CheckpointedPosition that = (CheckpointedPosition) o;
    return offset == that.offset &&
        recordsAfterOffset == that.recordsAfterOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, recordsAfterOffset);
  }

  @Override
  public String toString() {
    return String.format("CheckpointedPosition: offset=%s, recordsToSkip=%d",
        offset == NO_OFFSET ? "NO_OFFSET" : String.valueOf(offset),
        recordsAfterOffset);
  }
}

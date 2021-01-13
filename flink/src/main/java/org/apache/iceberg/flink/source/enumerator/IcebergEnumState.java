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

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;

/**
 * Enumerator state for checkpointing
 */
public class IcebergEnumState implements Serializable {

  private final Optional<Long> lastEnumeratedSnapshotId;
  private final Map<IcebergSourceSplit, IcebergSourceSplitState> pendingSplits;

  public IcebergEnumState(Map<IcebergSourceSplit, IcebergSourceSplitState> pendingSplits) {
    this(Optional.empty(), pendingSplits);
  }

  public IcebergEnumState(
      Optional<Long> lastEnumeratedSnapshotId,
      Map<IcebergSourceSplit, IcebergSourceSplitState> pendingSplits) {
    this.lastEnumeratedSnapshotId = lastEnumeratedSnapshotId;
    this.pendingSplits = pendingSplits;
  }

  public Optional<Long> lastEnumeratedSnapshotId() {
    return lastEnumeratedSnapshotId;
  }

  public Map<IcebergSourceSplit, IcebergSourceSplitState> pendingSplits() {
    return pendingSplits;
  }
}
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
import java.util.Optional;
import org.apache.iceberg.flink.source.assigner.SplitAssignerState;

/**
 * Enumerator state for checkpointing
 */
public class IcebergEnumState<SplitAssignerStateT extends SplitAssignerState> implements Serializable {

  private final Optional<Long> lastEnumeratedSnapshotId;
  private final SplitAssignerStateT assignerState;

  public IcebergEnumState(SplitAssignerStateT assignerState) {
    this(Optional.empty(), assignerState);
  }

  public IcebergEnumState(
      Optional<Long> lastEnumeratedSnapshotId,
      SplitAssignerStateT assignerState) {
    this.lastEnumeratedSnapshotId = lastEnumeratedSnapshotId;
    this.assignerState = assignerState;
  }

  public Optional<Long> lastEnumeratedSnapshotId() {
    return lastEnumeratedSnapshotId;
  }

  public SplitAssignerStateT assignerState() {
    return assignerState;
  }
}
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

package org.apache.iceberg.flink.source.split;

import org.apache.iceberg.CombinedScanTask;

/**
 * This essentially the mutable version of {@link IcebergSourceSplit}
 */
public class IcebergSourceSplitState {

  private final CombinedScanTask task;
  private long currentPosition;

  IcebergSourceSplitState(CombinedScanTask task, long currentPosition) {
    this.task = task;
    this.currentPosition = currentPosition;
  }

  public static IcebergSourceSplitState fromSplit(IcebergSourceSplit split) {
    return new IcebergSourceSplitState(split.task(), split.startingPosition());
  }

  public CombinedScanTask task() {
    return task;
  }

  public long currentPosition() {
    return currentPosition;
  }

  public void currentPosition(long position) {
    this.currentPosition = position;
  }
}

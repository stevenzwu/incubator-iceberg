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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

/**
 * This assigner hands out splits in a random order.
 * It doesn't guarantee any ordering.
 */
public class SimpleIcebergSplitAssigner implements IcebergSplitAssigner {

  private final Queue<IcebergSourceSplit> pendingSplits;

  public SimpleIcebergSplitAssigner() {
    pendingSplits = new ArrayDeque<>();
  }

  public SimpleIcebergSplitAssigner(
      Collection<IcebergSourceSplit> pendingSplits) {
    this.pendingSplits = new ArrayDeque<>(pendingSplits);
  }

  @Override
  public Optional<IcebergSourceSplit> getNext(int subtask) {
    return Optional.ofNullable(pendingSplits.poll());
  }

  @Override
  public void addSplits(Collection<IcebergSourceSplit> splits) {
    pendingSplits.addAll(splits);
  }

  @Override
  public Collection<IcebergSourceSplit> remainingSplits() {
    return pendingSplits;
  }

  @Override
  public void finishSplits(Collection<String> finishedSplitIds) {
    // no-op
  }
}

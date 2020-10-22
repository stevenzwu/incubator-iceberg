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

package org.apache.iceberg.flink.source.assigner;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

/**
 * This assigner hands out splits in a random order.
 * It doesn't guarantee any ordering.
 */
public class SimpleAssigner implements Assigner<SimpleAssignerState> {

  private final Queue<IcebergSourceSplit> pendingSplits;
  private final AtomicBoolean noMoreSplits;
  private final Queue<Integer> readersAwaitingSplit;

  public SimpleAssigner() {
    this(Collections.emptyList(), false);
  }

  public SimpleAssigner(
      Collection<IcebergSourceSplit> splits,
      boolean noMoreSplits) {
    this.pendingSplits = new ArrayDeque<>(splits);
    this.noMoreSplits = new AtomicBoolean(noMoreSplits);
    this.readersAwaitingSplit = new ArrayDeque<>();
  }

  @Override
  public void onSplitsCompletion(int subtask, Collection<String> completedSplitIds) {
    // no-op
  }

  @Override
  public Optional<SplitsAssignment> getAssignment(int subtask) {
    return null;
  }

  @Override
  public void addSplits(Collection<IcebergSourceSplit> splits) {
    pendingSplits.addAll(splits);
  }

  @Override
  public void onNoMoreSplits() {
    noMoreSplits.set(true);
  }

  @Override
  public SimpleAssignerState state() {
    return new SimpleAssignerState(pendingSplits, noMoreSplits.get());
  }
}

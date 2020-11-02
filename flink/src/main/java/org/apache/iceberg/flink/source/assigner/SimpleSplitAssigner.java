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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

/**
 * This assigner hands out splits without any guarantee in order or locality.
 *
 * Since all methods are executed by the source coordinator thread,
 * there is no need for locking.
 */
public class SimpleSplitAssigner implements SplitAssigner<SimpleSplitAssignerState> {

  private final Queue<IcebergSourceSplit> pendingSplits;
  private volatile boolean noMoreSplits;
  private final Map<Integer, Queue<CompletableFuture<IcebergSourceSplit>>> pendingFutures;

  public SimpleSplitAssigner() {
    this(new SimpleSplitAssignerState());
  }

  public SimpleSplitAssigner(SimpleSplitAssignerState state) {
    this.pendingSplits = new ArrayDeque<>(state.getPendingSplits());
    this.noMoreSplits = state.noMoreSplits();
    this.pendingFutures = new LinkedHashMap<>();
  }

  @Override
  public void addSplits(Collection<IcebergSourceSplit> splits) {
    pendingSplits.addAll(splits);
    // check pending futures to see if some can be completed now
    completePendingFuturesIfNeeded();
  }

  @Override
  public void onNoMoreSplits() {
    noMoreSplits = true;
    completePendingFuturesIfNeeded();
  }

  @Override
  public void onAddedReader(int subtaskId) {
    // clear up pending futures for the reader
    // maybe orphaned before the reader restart.
    pendingFutures.remove(subtaskId);
  }

  @Override
  public void onSplitsCompletion(int subtask, Collection<String> completedSplitIds) {
    // no-op
  }

  @Override
  public CompletableFuture<IcebergSourceSplit> getNext(int subtask) {
    CompletableFuture<IcebergSourceSplit> future = new CompletableFuture<>();
    IcebergSourceSplit split = pendingSplits.poll();
    if (split == null && !noMoreSplits) {
      // more splits may be discovered later
      pendingFutures.computeIfAbsent(subtask, ignored -> new ArrayDeque<>());
      pendingFutures.get(subtask).add(future);
    } else {
      // complete the future with a valid split or null (noMoreSplits)
      future.complete(split);
    }
    return future;
  }

  @Override
  public SimpleSplitAssignerState splitAssignerState() {
    return new SimpleSplitAssignerState(pendingSplits, noMoreSplits);
  }

  private void completePendingFuturesIfNeeded() {
    // first check if there are splits available to complete pending futures
    final Iterator<Map.Entry<Integer, Queue<CompletableFuture<IcebergSourceSplit>>>> awaitingReaderIter =
        pendingFutures.entrySet().iterator();
    while (!pendingSplits.isEmpty() && awaitingReaderIter.hasNext()) {
      final IcebergSourceSplit split = pendingSplits.poll();
      final Queue<CompletableFuture<IcebergSourceSplit>> futures = awaitingReaderIter.next().getValue();
      final CompletableFuture<IcebergSourceSplit> future = futures.poll();
      future.complete(split);
      if (futures.isEmpty()) {
        awaitingReaderIter.remove();
      }
    }
    // if pending splits queue is empty and noMoreSplits is true,
    // complete all remaining pending futures with null.
    while (noMoreSplits && awaitingReaderIter.hasNext()) {
      final Queue<CompletableFuture<IcebergSourceSplit>> futures = awaitingReaderIter.next().getValue();
      while (!futures.isEmpty()) {
        futures.poll().complete(null);
      }
      awaitingReaderIter.remove();
    }
  }
}

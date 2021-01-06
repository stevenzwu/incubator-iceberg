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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;

public interface SplitAssigner extends AutoCloseable {

  /**
   * Some assigners may need to start background threads or perform other activity such as
   * registering as listeners to updates from other event sources e.g., watermark tracker.
   */
  default void start() {
  }

  /**
   * Some assigners may need to perform certain actions
   * when their corresponding enumerators are closed
   */
  @Override
  default void close() {
  }

  /**
   * Request a new split from the assigner
   * as enumerator trying to assign splits to awaiting readers
   */
  GetSplitResult getNext(@Nullable String hostname);

  /**
   * Add new splits discovered by enumerator
   */
  void onDiscoveredSplits(Collection<IcebergSourceSplit> splits);

  /**
   *   Forward addSplitsBack event (for failed reader) to assigner
   */
  void onUnassignedSplits(Collection<IcebergSourceSplit> splits, int subtaskId);

  /**
   * Some assigner (like event time alignment) may rack in-progress splits
   * to advance watermark upon completed splits
   */
  default void onCompletedSplits(Collection<String> completedSplitIds, int subtaskId) {
  }

  /**
   * Get assigner state for checkpointing.
   * This is a super-set API that works for all currently imagined assigners.
   */
  Map<IcebergSourceSplit, IcebergSourceSplitState> checkpointState();

  /**
   * Enumerator can get a notification via CompletableFuture
   * when the assigner has more splits available later.
   * Enumerator should schedule assignment in the thenAccept action of the future.
   */
  CompletableFuture<Void> isAvailable();
}

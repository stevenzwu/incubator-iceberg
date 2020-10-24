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
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public interface SplitAssigner<SplitAssignerStateT extends SplitAssignerState> {

  /**
   * Adds a set of splits to this assigner. This could happen when
   * <ul>
   *   <li>some split processing failed and the splits need to be re-added.
   *   <li>new splits got discovered.
   * </ul>
   */
  void addSplits(Collection<IcebergSourceSplit> splits);

  /**
   * Signal assigner that no more splits
   * will be discovered.
   */
  void onNoMoreSplits();

  /**
   * Notify assigner when splits are finished
   * in case assigner needs to do some bookkeeping work.
   * E.g. Event time aligned assinger may need to advance the watermark
   * based on the completed splits.
   */
  void onSplitsCompletion(int subtask, Collection<String> completedSplitIds);

  /**
   * Request the next split .
   *
   * The returned future is guaranteed to be completed with
   * either a valid split or null (indicating no more splits).
   */
  CompletableFuture<IcebergSourceSplit> getNext(int subtask);

  /**
   * Gets the split assigner state for checkpointing
   */
  SplitAssignerStateT splitAssignerState();

}

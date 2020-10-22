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

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public interface Assigner<AssignerStateT extends AssignerState> {

  /**
   * Notify assigner when splits are finished
   * in case assigner needs to do some bookkeeping work.
   * E.g. EventTimeAlignedSplitAssigner may need up advance the watermark.
   */
  void onSplitsCompletion(int subtask, Collection<String> completedSplitIds);

  /**
   * This can be called when reader requests more splits
   * <li>Enumerator discovered new splits
   * <p>
   *
   * The assignment result could be one of those three scenarios
   * <p>
   * <ul>
   *   <li>Splits are assigned only for the requesting subtask
   *   <li>No splits are assigned for the requesting subtask and
   *   assigner should keep track of the readers waiting for assignment.
   *     <ul>
   *       <li>Assigner doesn't have splits available now.
   *       But more splits may be available later.
   *       <li>Assigner may decide to hold back the assignment
   *       due to some constraint (e.g. event time alignment)
   *     </ul>
   *   <li>Splits are assigned for multiple readers/subtasks.
   *   Assigner should assign splits to the waiting readers
   *   (along with the requesting reader)
   *   when splits become available or when the constraint is satisfied later.
   * </ul>
   */
  Optional<SplitsAssignment> getAssignment(int subtask);

  Optional<SplitsAssignment> getAssignment();

  /**
   * Adds a set of splits to this assigner. This could happen when
   * <ul>
   *   <li>some split processing failed and the splits need to be re-added.
   *   <li>new splits got discovered.
   * </ul>
   */
  void addSplits(Collection<IcebergSourceSplit> splits);

  /**
   * Signal assigner that there will be more splits
   * to be discovered.
   */
  void onNoMoreSplits();

  /**
   * Gets the split assigner state for checkpointing
   */
  AssignerStateT state();

  /**
   * This factory is to allow the {@code IcebergSplitAssigner} to be lazily
   * initialized and to not be serializable.
   */
  @FunctionalInterface
  interface Provider extends Serializable {

    /**
     * Creates a new {@code FileSplitAssigner} that starts with the given set of initial splits.
     */
    Assigner create();
  }
}

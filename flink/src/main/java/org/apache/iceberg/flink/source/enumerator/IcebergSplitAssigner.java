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
import java.util.Collection;
import java.util.Optional;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public interface IcebergSplitAssigner {

  /**
   * Gets the next split.
   *
   * <p>When this method returns an empty {@code Optional}, then the set of splits is
   * assumed to be done and the source will finish once the readers finished their current
   * splits.
   */
  Optional<IcebergSourceSplit> getNext(int subttask);

  /**
   * Adds a set of splits to this assigner. This happens for example when some split processing
   * failed and the splits need to be re-added, or when new splits got discovered.
   */
  void addSplits(Collection<IcebergSourceSplit> splits);

  /**
   * Gets the remaining splits that this assigner has pending.
   */
  Collection<IcebergSourceSplit> remainingSplits();

  /**
   * Perform any bookkeeping work for finished splits,
   * e.g. update watermark related to ordering and event time alignment.
   */
  void finishSplits(Collection<String> finishedSplitIds);


  /**
   * This factory is to allow the {@code IcebergSplitAssigner} to be lazily
   * initialized and to not be serializable.
   */
  @FunctionalInterface
  interface Provider extends Serializable {

    /**
     * Creates a new {@code FileSplitAssigner} that starts with the given set of initial splits.
     */
    IcebergSplitAssigner create();
  }
}

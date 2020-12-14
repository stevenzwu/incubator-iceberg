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

import java.util.Optional;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-time split enumeration at the beginning
 */
public class StaticIcebergSplitEnumerator<SplitAssignerStateT extends SplitAssignerState>
    extends AbstractIcebergEnumerator<SplitAssignerStateT> {
  private static final Logger LOG = LoggerFactory.getLogger(StaticIcebergSplitEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumContext;
  private final SplitAssigner<SplitAssignerStateT> assigner;

  public StaticIcebergSplitEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      SplitAssigner<SplitAssignerStateT> assigner) {
    super(enumContext, assigner);
    this.enumContext = enumContext;
    this.assigner = assigner;
  }

  @Override
  public void start() {
    // already discovered all the splits and added to assigner
    // no resources to start
  }

  @Override
  public IcebergEnumState<SplitAssignerStateT> snapshotState() throws Exception {
    return new IcebergEnumState<>(
        Optional.empty(),
        assigner.splitAssignerState());
  }
}

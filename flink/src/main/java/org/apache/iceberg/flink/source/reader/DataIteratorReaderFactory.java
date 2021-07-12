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

package org.apache.iceberg.flink.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.CloseableIterator;

public abstract class DataIteratorReaderFactory<T> implements ReaderFactory<T> {

  private final Configuration config;
  private final DataIteratorBatcher<T> batcher;

  public DataIteratorReaderFactory(Configuration config, DataIteratorBatcher<T> batcher) {
    this.config = config;
    this.batcher = batcher;
  }

  protected abstract DataIterator<T> createDataIterator(IcebergSourceSplit split);

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> apply(IcebergSourceSplit split) {
    DataIterator<T> inputIterator = createDataIterator(split);
    if (split.position() != null) {
      inputIterator.seek(split.position());
    }
    return batcher.apply(split.splitId(), inputIterator);
  }

}

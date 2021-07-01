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

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataIterator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public class RowDataIteratorReaderFactory implements ReaderFactory<RowData> {

  private final Table table;
  private final ScanContext scanContext;
  private final RowType rowType;
  private final TypeSerializer[] fieldSerializers;

  public RowDataIteratorReaderFactory(
      Table table,
      ScanContext scanContext,
      RowType rowType) {
    this.table = table;
    this.scanContext = scanContext;
    this.rowType = rowType;
    this.fieldSerializers = rowType.getChildren().stream()
        .map(InternalSerializers::create)
        .toArray(TypeSerializer[]::new);
  }

  @Override
  public ReaderFactory.Reader<RowData> create(Configuration config, IcebergSourceSplit split) {
    final DataIterator<RowData> inputIterator = new RowDataIterator(
        split.task(),
        table.io(),
        table.encryption(),
        table.schema(),
        scanContext.project(),
        scanContext.nameMapping(),
        scanContext.caseSensitive());
    if (split.checkpointedPosition() != null) {
      inputIterator.seek(split.checkpointedPosition());
    }
    return new RowDataIteratorReader(config, inputIterator);
  }

  public class RowDataIteratorReader implements ReaderFactory.Reader<RowData> {

    private final DataIterator<RowData> inputIterator;
    private final int batchSize;
    private final Pool<RowData[]> pool;

    public RowDataIteratorReader(Configuration config, DataIterator<RowData> inputIterator) {
      this.inputIterator = inputIterator;
      this.batchSize = config.getInteger(FlinkConfigOptions.SOURCE_READER_FETCH_BATCH_SIZE);
      this.pool = createPoolOfBatches(config.getInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY));
    }

    @Override
    public ReaderFactory.RecordIterator<RowData> readBatch() throws IOException {
      final RowData[] batch = getCachedEntry();
      int num = 0;
      while (inputIterator.hasNext() && num < batchSize) {
        RowData nextRecord = inputIterator.next();
        RowDataUtil.clone(nextRecord, batch[num], rowType, fieldSerializers);
        num++;
        if (inputIterator.isCurrentIteratorDone()) {
          // break early so that records in the ArrayResultIterator
          // have the same fileOffset.
          break;
        }
      }
      if (num == 0) {
        return null;
      } else {
        DataIterator.Position position = inputIterator.position();
        return new RecyclableArrayIterator<RowData>(pool.recycler(), batch,
            num, position.fileOffset(), position.recordOffset() - num);

      }
    }

    @Override
    public void close() throws IOException {
      if (inputIterator != null) {
        inputIterator.close();
      }
    }

    private Pool<RowData[]> createPoolOfBatches(int numBatches) {
      final Pool<RowData[]> poolOfBatches = new Pool<>(numBatches);
      for (int batchId = 0; batchId < numBatches; batchId++) {
        RowData[] arr = new RowData[batchSize];
        for (int i = 0; i < batchSize; ++i) {
          arr[i] = new GenericRowData(rowType.getFieldCount());
        }
        poolOfBatches.add(arr);
      }
      return poolOfBatches;
    }

    private RowData[] getCachedEntry() throws IOException {
      try {
        return pool.pollEntry();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted");
      }
    }
  }
}

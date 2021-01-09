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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.FlinkSplitGenerator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFileFormatAdaptor {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final FileFormat fileFormat = FileFormat.PARQUET;
  private static final ScanContext scanContext = new ScanContext()
      .project(TestFixtures.SCHEMA);
  private static final RowType rowType = FlinkSchemaUtil
      .convert(scanContext.projectedSchema());
  private static final DataStructureConverter<Object, Object> rowDataConverter = DataStructureConverters.getConverter(
      TypeConversions.fromLogicalToDataType(rowType));
  private static final org.apache.flink.configuration.Configuration flinkConfig =
      new org.apache.flink.configuration.Configuration();
  private static final FileFormatAdaptor<RowData> fileFormatAdaptor = new FileFormatAdaptor<>(
      new ParquetColumnarRowInputFormat(new Configuration(),
          rowType, 128, false, true));

  private static String warehouse;
  private static HadoopCatalog catalog;
  private static Table table;
  private static GenericAppenderHelper dataAppender;

  private static List<Record> recordBatch1;
  private static List<Record> recordBatch2;
  private static List<Record> recordBatch3;
  private static IcebergSourceSplit icebergSplit;

  @BeforeClass
  public static void beforeClass() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    catalog = new HadoopCatalog(hadoopConf, warehouse);
    table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    dataAppender = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    // snapshot1
    recordBatch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch1);
    // snapshot2
    recordBatch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch2);
    // snapshot3
    recordBatch3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch3);

    final List<IcebergSourceSplit> splits = FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
    Assert.assertEquals(1, splits.size());
    icebergSplit = splits.get(0);
    Assert.assertEquals(3, icebergSplit.task().files().size());
  }

  @AfterClass
  public static void afterClass() throws IOException {
    catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
    catalog.close();
  }

  @Test
  public void testNoCheckpointedPosition() throws IOException {
    final BulkFormat.Reader<RowData> reader = fileFormatAdaptor.createReader(flinkConfig, icebergSplit);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 0L, 0L);
    TestHelpers.assertRecords(rows1, recordBatch1, TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> rows2 = toRows(iter2, 1L, 0L);
    TestHelpers.assertRecords(rows2, recordBatch2, TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter3 = reader.readBatch();
    final List<Row> rows3 = toRows(iter3, 2L, 0L);
    TestHelpers.assertRecords(rows3, recordBatch3, TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionBeforeFirstFile() throws IOException {
    final IcebergSourceSplit checkpointedSplit = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(0L, 0L));
    final BulkFormat.Reader<RowData> reader = fileFormatAdaptor.restoreReader(flinkConfig, checkpointedSplit);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 0L, 0L);
    TestHelpers.assertRecords(rows1, recordBatch1, TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> rows2 = toRows(iter2, 1L, 0L);
    TestHelpers.assertRecords(rows2, recordBatch2, TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter3 = reader.readBatch();
    final List<Row> rows3 = toRows(iter3, 2L, 0L);
    TestHelpers.assertRecords(rows3, recordBatch3, TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionMiddleFirstFile() throws IOException {
    final IcebergSourceSplit checkpointedSplit = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(0L, 1L));
    final BulkFormat.Reader<RowData> reader = fileFormatAdaptor.restoreReader(flinkConfig, checkpointedSplit);

    final BulkFormat.RecordIterator<RowData> iter1 = reader.readBatch();
    final List<Row> rows1 = toRows(iter1, 0L, 1L);
    TestHelpers.assertRecords(rows1, recordBatch1.subList(1, 2), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> rows2 = toRows(iter2, 1L, 0L);
    TestHelpers.assertRecords(rows2, recordBatch2, TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter3 = reader.readBatch();
    final List<Row> rows3 = toRows(iter3, 2L, 0L);
    TestHelpers.assertRecords(rows3, recordBatch3, TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionAfterFirstFile() throws IOException {
    final IcebergSourceSplit checkpointedSplit = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(0L, 2L));
    final BulkFormat.Reader<RowData> reader = fileFormatAdaptor.restoreReader(flinkConfig, checkpointedSplit);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> rows2 = toRows(iter2, 1L, 0L);
    TestHelpers.assertRecords(rows2, recordBatch2, TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter3 = reader.readBatch();
    final List<Row> rows3 = toRows(iter3, 2L, 0L);
    TestHelpers.assertRecords(rows3, recordBatch3, TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionBeforeSecondFile() throws IOException {
    final IcebergSourceSplit checkpointedSplit = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(1L, 0L));
    final BulkFormat.Reader<RowData> reader = fileFormatAdaptor.restoreReader(flinkConfig, checkpointedSplit);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> rows2 = toRows(iter2, 1L, 0L);
    TestHelpers.assertRecords(rows2, recordBatch2, TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter3 = reader.readBatch();
    final List<Row> rows3 = toRows(iter3, 2L, 0L);
    TestHelpers.assertRecords(rows3, recordBatch3, TestFixtures.SCHEMA);
  }

  @Test
  public void testCheckpointedPositionMidSecondFile() throws IOException {
    final IcebergSourceSplit checkpointedSplit = new IcebergSourceSplit(
        icebergSplit.task(),
        new CheckpointedPosition(1L, 1L));
    final BulkFormat.Reader<RowData> reader = fileFormatAdaptor.restoreReader(flinkConfig, checkpointedSplit);

    final BulkFormat.RecordIterator<RowData> iter2 = reader.readBatch();
    final List<Row> rows2 = toRows(iter2, 1L, 1L);
    TestHelpers.assertRecords(rows2, recordBatch2.subList(1, 2), TestFixtures.SCHEMA);

    final BulkFormat.RecordIterator<RowData> iter3 = reader.readBatch();
    final List<Row> rows3 = toRows(iter3, 2L, 0L);
    TestHelpers.assertRecords(rows3, recordBatch3, TestFixtures.SCHEMA);
  }

  private List<Row> toRows(final BulkFormat.RecordIterator<RowData> iter,
                           final long exptectedFileOffset,
                           final long startRecordOffset) {
    if (iter == null) {
      return Collections.emptyList();
    }
    final List<Row> result = new ArrayList<>();
    RecordAndPosition<RowData> recordAndPosition;
    long recordOffset = startRecordOffset;
    while ((recordAndPosition = iter.next()) != null) {
      Assert.assertEquals(exptectedFileOffset, recordAndPosition.getOffset());
      Assert.assertEquals(recordOffset, recordAndPosition.getRecordSkipCount() - 1);
      recordOffset++;
      final Row row = (Row) rowDataConverter.toExternal(recordAndPosition.getRecord());
      result.add(row);
    }
    return result;
  }
}

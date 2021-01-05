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
import java.util.Arrays;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableInfo;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.FlinkSplitGenerator;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.MutableIcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSourceSplitReader {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private final FileFormat fileFormat = FileFormat.PARQUET;
  private final ScanContext scanContext = new ScanContext()
      .project(TestFixtures.SCHEMA);

  private String warehouse;
  private HadoopCatalog catalog;
  private Table table;
  private GenericAppenderHelper dataAppender;

  @Before
  public void before() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    catalog = new HadoopCatalog(hadoopConf, warehouse);
    table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    dataAppender = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
  }

  @After
  public void after() throws IOException {
    catalog.close();
  }

  @Test
  public void testFullScan() throws Exception {
    // snapshot1
    final List<Record> recordBatch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch1);
    // snapshot2
    final List<Record> recordBatch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch2);
    // snapshot3
    final List<Record> recordBatch3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch3);

    final List<IcebergSourceSplit> splits = FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(3, splits.get(0).task().files().size());
    final IcebergSourceSplit split = splits.get(0);

    final Configuration config = new Configuration();
    RowType rowType = FlinkSchemaUtil.convert(table.schema());
    BulkFormat<RowData, IcebergSourceSplit> bulkFormat = new RowDataIteratorBulkFormat(
        TableInfo.fromTable(table), scanContext, rowType);
    IcebergSourceSplitReader reader = new IcebergSourceSplitReader(config, bulkFormat);
    reader.handleSplitsChanges(new SplitsAddition(Arrays.asList(split)));

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch1
        = reader.fetch();
    final List<Row> rowBatch1 = readRows(readBatch1, split.splitId(), 0L, 0L);
    TestHelpers.assertRecords(rowBatch1, recordBatch1, TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch2
        = reader.fetch();
    final List<Row> rowBatch2 = readRows(readBatch2, split.splitId(), 1L, 0L);
    TestHelpers.assertRecords(rowBatch2, recordBatch2, TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch3
        = reader.fetch();
    final List<Row> rowBatch3 = readRows(readBatch3, split.splitId(), 2L, 0L);
    TestHelpers.assertRecords(rowBatch3, recordBatch3, TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> finishedBatch
        = reader.fetch();
    Assert.assertEquals(Sets.newHashSet(split.splitId()), finishedBatch.finishedSplits());
  }

  @Test
  public void testResumeFromEndOfFirstBatch() throws Exception {
    // snapshot1
    final List<Record> recordBatch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch1);
    // snapshot2
    final List<Record> recordBatch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch2);
    // snapshot3
    final List<Record> recordBatch3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch3);

    final List<IcebergSourceSplit> splits = FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(3, splits.get(0).task().files().size());

    final IcebergSourceSplit split = IcebergSourceSplit.fromSplitState(
        new MutableIcebergSourceSplit(splits.get(0).task(), 0L, 2L));

    final Configuration config = new Configuration();
    RowType rowType = FlinkSchemaUtil.convert(table.schema());
    BulkFormat<RowData, IcebergSourceSplit> bulkFormat = new RowDataIteratorBulkFormat(
        TableInfo.fromTable(table), scanContext, rowType);
    IcebergSourceSplitReader reader = new IcebergSourceSplitReader(config, bulkFormat);
    reader.handleSplitsChanges(new SplitsAddition(Arrays.asList(split)));

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch2
        = reader.fetch();
    final List<Row> rowBatch2 = readRows(readBatch2, split.splitId(), 1L, 0L);
    TestHelpers.assertRecords(rowBatch2, recordBatch2, TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch3
        = reader.fetch();
    final List<Row> rowBatch3 = readRows(readBatch3, split.splitId(), 2L, 0L);
    TestHelpers.assertRecords(rowBatch3, recordBatch3, TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> finishedBatch
        = reader.fetch();
    Assert.assertEquals(Sets.newHashSet(split.splitId()), finishedBatch.finishedSplits());
  }

  @Test
  public void testResumeFromStartOfSecondBatch() throws Exception {
    // snapshot1
    final List<Record> recordBatch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch1);
    // snapshot2
    final List<Record> recordBatch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch2);
    // snapshot3
    final List<Record> recordBatch3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch3);

    final List<IcebergSourceSplit> splits = FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(3, splits.get(0).task().files().size());

    final IcebergSourceSplit split = IcebergSourceSplit.fromSplitState(
        new MutableIcebergSourceSplit(splits.get(0).task(), 1L, 0L));

    final Configuration config = new Configuration();
    RowType rowType = FlinkSchemaUtil.convert(table.schema());
    BulkFormat<RowData, IcebergSourceSplit> bulkFormat = new RowDataIteratorBulkFormat(
        TableInfo.fromTable(table), scanContext, rowType);
    IcebergSourceSplitReader reader = new IcebergSourceSplitReader(config, bulkFormat);
    reader.handleSplitsChanges(new SplitsAddition(Arrays.asList(split)));

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch2
        = reader.fetch();
    final List<Row> rowBatch2 = readRows(readBatch2, split.splitId(), 1L, 0L);
    TestHelpers.assertRecords(rowBatch2, recordBatch2, TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch3
        = reader.fetch();
    final List<Row> rowBatch3 = readRows(readBatch3, split.splitId(), 2L, 0L);
    TestHelpers.assertRecords(rowBatch3, recordBatch3, TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> finishedBatch
        = reader.fetch();
    Assert.assertEquals(Sets.newHashSet(split.splitId()), finishedBatch.finishedSplits());
  }

  @Test
  public void testResumeFromMiddleOfSecondBatch() throws Exception {
    // snapshot1
    final List<Record> recordBatch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch1);
    // snapshot2
    final List<Record> recordBatch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch2);
    // snapshot3
    final List<Record> recordBatch3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(recordBatch3);

    final List<IcebergSourceSplit> splits = FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(3, splits.get(0).task().files().size());

    final IcebergSourceSplit split = IcebergSourceSplit.fromSplitState(
        new MutableIcebergSourceSplit(splits.get(0).task(), 1L, 1L));

    final Configuration config = new Configuration();
    RowType rowType = FlinkSchemaUtil.convert(table.schema());
    BulkFormat<RowData, IcebergSourceSplit> bulkFormat = new RowDataIteratorBulkFormat(
        TableInfo.fromTable(table), scanContext, rowType);
    IcebergSourceSplitReader reader = new IcebergSourceSplitReader(config, bulkFormat);
    reader.handleSplitsChanges(new SplitsAddition(Arrays.asList(split)));

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch2
        = reader.fetch();
    final List<Row> rowBatch2 = readRows(readBatch2, split.splitId(), 1L, 1L);
    TestHelpers.assertRecords(rowBatch2, recordBatch2.subList(1, 2), TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch3
        = reader.fetch();
    final List<Row> rowBatch3 = readRows(readBatch3, split.splitId(), 2L, 0L);
    TestHelpers.assertRecords(rowBatch3, recordBatch3, TestFixtures.SCHEMA);

    final RecordsWithSplitIds<RecordAndPosition<RowData>> finishedBatch
        = reader.fetch();
    Assert.assertEquals(Sets.newHashSet(split.splitId()), finishedBatch.finishedSplits());
  }

  private List<Row> readRows(
      RecordsWithSplitIds<RecordAndPosition<RowData>> readBatch,
      String expectedSplitId, long expectedOffset, long expectedStartingRecordOffset) {
    Assert.assertEquals(expectedSplitId, readBatch.nextSplit());
    final List<RowData> rowDataList = new ArrayList<>();
    RecordAndPosition<RowData> row;
    int num = 0;
    while ((row = readBatch.nextRecordFromSplit()) != null) {
      Assert.assertEquals(expectedOffset, row.getOffset());
      num++;
      Assert.assertEquals(expectedStartingRecordOffset + num, row.getRecordSkipCount());
      rowDataList.add(row.getRecord());
    }
    readBatch.recycle();
    return TestHelpers.convertRowDataToRow(rowDataList, TestFixtures.ROW_TYPE);
  }

}

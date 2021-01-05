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

package org.apache.iceberg.flink.source;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.ContinuousEnumConfig;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestIcebergSourceContinuous extends AbstractTestBase {

  private HadoopCatalog catalog;
  private String warehouse;
  private String location;
  private TableLoader tableLoader;

  // parametrized variables
  private final FileFormat fileFormat = FileFormat.PARQUET;

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
    location = String.format("%s/%s/%s", warehouse, TestFixtures.DATABASE, TestFixtures.TABLE);
    tableLoader = TableLoader.fromHadoopTable(location);

    table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    dataAppender = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
  }

  @After
  public void after() throws IOException {
    catalog.close();
  }

  // need latest change in DataStreamUtils
  @Ignore
  @Test
  public void testTableScanThenIncremental() throws Exception {
    final RowType rowType = FlinkSchemaUtil.convert(table.schema());

    // snapshot1
    List<Record> batch1 = RandomGenericData.generate(table.schema(), 2, 0L);
    dataAppender.appendToTable(batch1);
    long snapshotId1 = table.currentSnapshot().snapshotId();

    // start the source and collect output
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    final DataStream<Row> stream = env.fromSource(
        IcebergSource.builder()
            .tableLoader(tableLoader)
            .assignerFactory(new SimpleSplitAssignerFactory())
            .bulkFormat(null)
            .scanContext(new ScanContext()
                .project(table.schema()))
            .continuousEnumSettings(ContinuousEnumConfig.builder()
                .discoveryInterval(Duration.ofMillis(10L))
                .startingStrategy(ContinuousEnumConfig.StartingStrategy.LATEST_SNAPSHOT)
                .build())
            .build(),
        WatermarkStrategy.noWatermarks(),
        "icebergSource",
        TypeInformation.of(RowData.class))
        .map(new RowDataToRowMapper(rowType));

//    final DataStreamUtils.ClientAndIterator<Row> client =
//        DataStreamUtils.collectWithClient(stream, "Continuous Iceberg Source Test");
//
//    final List<Row> result1 = DataStreamUtils.collectRecordsFromUnboundedStream(client, 2);
//    TestFlinkScan.assertRecords(result1, batch1, table.schema());
//
//    // shut down the job, now that we have all the results we expected.
//    client.client.cancel().get();
  }
}

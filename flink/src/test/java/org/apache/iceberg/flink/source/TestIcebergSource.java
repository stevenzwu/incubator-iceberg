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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.reader.RowDataIteratorFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergSource extends TestFlinkScan {

  public TestIcebergSource(String fileFormat) {
    super(fileFormat);
  }

  @Override
  protected List<Row> runWithProjection(String... projected) throws Exception {
    final Schema tableSchema = tableLoader.loadTable().schema();
    final Schema projectedSchema = tableSchema.select(projected);
    ScanContext scanContext = new ScanContext()
        .project(projectedSchema);
    return run(getBuilder(scanContext));
  }

  @Override
  protected List<Row> runWithFilter(Expression filter, String sqlFilter) throws Exception {
    ScanContext scanContext = new ScanContext()
        .project(SCHEMA)
        .filterRows(Arrays.asList(filter));
    return run(getBuilder(scanContext));
  }

  @Override
  protected List<Row> runWithOptions(Map<String, String> options) throws Exception {
    ScanContext scanContext = new ScanContext()
        .project(SCHEMA)
        .fromProperties(options);
    return run(getBuilder(scanContext));
  }

  @Override
  protected List<Row> run() throws Exception {
    return run(getBuilder(new ScanContext().project(SCHEMA)));
  }

  private IcebergSource.Builder<RowData> getBuilder(ScanContext scanContext) {
    return IcebergSource.<RowData>builder(tableLoader)
        .iteratorFactory(new RowDataIteratorFactory())
        .scanContext(scanContext);
  }

  private List<Row> run(IcebergSource.Builder<RowData> sourceBuilder) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    final IcebergSource<RowData> source = sourceBuilder.build();

    final DataStream<RowData> stream = env.fromSource(
        source,
        WatermarkStrategy.noWatermarks(),
        "testBasicRead",
        TypeInformation.of(RowData.class));

    final List<RowData> result = ImmutableList.copyOf(DataStreamUtils.collect(stream));
    final RowType rowType = FlinkSchemaUtil.convert(source.scanContext().projectedSchema());
    return TestHelpers.convertRowDataToRow(result, rowType);
  }

//  private List<Row> run(IcebergSource.Builder<RowData> sourceBuilder) throws Exception {
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(1);
//    final IcebergSource<RowData> source = sourceBuilder.build();
//
//    final RichMapFunction<RowData, Row> converter = new RichMapFunction<RowData, Row>() {
//
//      private transient DataStructureConverter<Object, Object> converter;
//
//      @Override
//      public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
//        final RowType rowType = FlinkSchemaUtil.convert(source.tableInfo().schema());
//        converter = DataStructureConverters.getConverter(
//            TypeConversions.fromLogicalToDataType(rowType));
//      }
//
//      @Override
//      public Row map(RowData rowData) throws Exception {
//        return (Row) converter.toExternal(rowData);
//      }
//    };
//
//    final DataStream<Row> stream = env.fromSource(
//        source,
//        WatermarkStrategy.noWatermarks(),
//        "testBasicRead",
//        TypeInformation.of(RowData.class))
//        .map(converter);
//
//    final List<Row> result = new ArrayList<>(ImmutableList.copyOf(DataStreamUtils.collect(stream)));
//    return result;
//  }
}

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergSource extends TestFlinkScan {

  TestIcebergSource(String fileFormat) {
    super(fileFormat);
  }

  protected List<Row> runWithProjection(String... projected) throws IOException {
    TableSchema.Builder builder = TableSchema.builder();
    TableSchema schema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(
        catalog.loadTable(TableIdentifier.of("default", "t")).schema()));
    for (String field : projected) {
      TableColumn column = schema.getTableColumn(field).get();
      builder.field(column.getName(), column.getType());
    }
    return run(FlinkSource.forRowData().project(builder.build()), Maps.newHashMap(), "", projected);
  }

  protected List<Row> runWithFilter(Expression filter, String sqlFilter) throws IOException {
    ScanContext scanContext = new ScanContext();
    scanContext.filterRows(Arrays.asList(filter));
    IcebergSource.Builder builder = FlinkSource.forRowData().filters(Collections.singletonList(filter));
    return run(builder, Maps.newHashMap(), sqlFilter, "*");
  }

  protected List<Row> runWithOptions(Map<String, String> options) throws IOException {
    FlinkSource.Builder builder = FlinkSource.forRowData();
    Optional.ofNullable(options.get("snapshot-id")).ifPresent(value -> builder.snapshotId(Long.parseLong(value)));
    Optional.ofNullable(options.get("start-snapshot-id"))
        .ifPresent(value -> builder.startSnapshotId(Long.parseLong(value)));
    Optional.ofNullable(options.get("end-snapshot-id"))
        .ifPresent(value -> builder.endSnapshotId(Long.parseLong(value)));
    Optional.ofNullable(options.get("as-of-timestamp"))
        .ifPresent(value -> builder.asOfTimestamp(Long.parseLong(value)));
    return run(builder, options, "", "*");
  }

  protected List<Row> run() throws IOException {
    return run(FlinkSource.forRowData(), Maps.newHashMap(), "", "*");
  }

  protected List<Row> run(IcebergSource.Builder sourceBuilder, Map<String, String> sqlOptions, String sqlFilter,
                                   String... sqlSelectedFields) throws IOException {

  }

}

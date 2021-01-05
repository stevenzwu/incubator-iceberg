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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableInfo;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.ContinuousEnumConfig;
import org.apache.iceberg.flink.source.enumerator.ContinuousIcebergEnumerator;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumState;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumStateSerializer;
import org.apache.iceberg.flink.source.enumerator.StaticIcebergEnumerator;
import org.apache.iceberg.flink.source.reader.IcebergSourceReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;

@Experimental
public class IcebergSource<T> implements Source<T, IcebergSourceSplit, IcebergEnumState> {

  private final TableLoader tableLoader;
  private final ContinuousEnumConfig contEnumSettings;
  private final ScanContext scanContext;
  private final BulkFormat<T, IcebergSourceSplit> bulkFormat;
  private final SplitAssignerFactory assignerFactory;

  private final TableInfo tableInfo;

  IcebergSource(
      TableLoader tableLoader,
      @Nullable ContinuousEnumConfig contEnumSettings,
      ScanContext scanContext,
      BulkFormat<T, IcebergSourceSplit> bulkFormat,
      SplitAssignerFactory assignerFactory) {

    this.tableLoader = tableLoader;
    this.contEnumSettings = contEnumSettings;
    this.scanContext = scanContext;
    this.bulkFormat = bulkFormat;
    this.assignerFactory = assignerFactory;

    // extract serializable table info once during construction
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      this.tableInfo = TableInfo.fromTable(loader.loadTable());
    } catch (IOException e) {
      throw new RuntimeException("Failed to close table loader", e);
    }
  }

  @Override
  public Boundedness getBoundedness() {
    return contEnumSettings == null ?
        Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
  }

  /**
   * TODO: we can simplify this method when the latest changes
   * (in flink-connector-base) are released
   */
  @Override
  public org.apache.flink.api.connector.source.SourceReader createReader(SourceReaderContext readerContext) {
    // we should be able to getConfiguration from SourceReaderContext in the future
    return new IcebergSourceReader(
        readerContext,
        bulkFormat);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext) {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumState> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext, IcebergEnumState enumState)
      throws IOException {
    return createEnumerator(enumContext, enumState);
  }

  @Override
  public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
    return IcebergSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<IcebergEnumState> getEnumeratorCheckpointSerializer() {
    return IcebergEnumStateSerializer.INSTANCE;
  }

  private SplitEnumerator<IcebergSourceSplit, IcebergEnumState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      @Nullable IcebergEnumState enumState) {

    final SplitAssigner assigner;
    if (enumState == null) {
      assigner = assignerFactory.createAssigner();
    } else {
      assigner = assignerFactory.createAssigner(enumState.pendingSplits());
    }
    if (contEnumSettings == null) {
      assigner.onDiscoveredSplits(getStaticSplits(tableLoader, scanContext));
      return new StaticIcebergEnumerator(
          enumContext,
          assigner);
    } else {
      return new ContinuousIcebergEnumerator(
          enumContext,
          tableLoader,
          scanContext,
          assigner,
          enumState,
          contEnumSettings);
    }
  }

  private static List<IcebergSourceSplit> getStaticSplits(
      TableLoader tableLoader, ScanContext scanContext) {
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      final Table table = loader.loadTable();
      return FlinkSplitGenerator.planIcebergSourceSplits(table, scanContext);
    } catch (IOException e) {
      throw new RuntimeException("Failed to close table loader", e);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder<T> {

    // required
    private TableLoader tableLoader;
    private SplitAssignerFactory splitAssignerFactory;
    private BulkFormat<T, IcebergSourceSplit> bulkFormat;

    // optional
    private Configuration config;
    private ScanContext scanContext;
    @Nullable private ContinuousEnumConfig contEnumSettings;

    Builder() {
      this.config = new Configuration();
      this.scanContext = new ScanContext();
    }

    public Builder<T> tableLoader(TableLoader loader) {
      this.tableLoader = loader;
      return this;
    }

    public Builder<T> assignerFactory(SplitAssignerFactory assignerFactory) {
      this.splitAssignerFactory = assignerFactory;
      return this;
    }

    public Builder<T> bulkFormat(BulkFormat<T, IcebergSourceSplit> newBulkFormat) {
      this.bulkFormat = newBulkFormat;
      return this;
    }

    public Builder<T> config(Configuration newConfig) {
      this.config = newConfig;
      return this;
    }

    public Builder<T> scanContext(ScanContext newScanContext) {
      this.scanContext = newScanContext;
      return this;
    }

    public Builder<T> continuousEnumSettings(ContinuousEnumConfig newContEnumSettings) {
      this.contEnumSettings = newContEnumSettings;
      return this;
    }

    public IcebergSource<T> build() {
      checkRequired();
      return new IcebergSource(
          tableLoader,
          contEnumSettings,
          scanContext,
          bulkFormat,
          splitAssignerFactory);
    }

    private void checkRequired() {
      Preconditions.checkNotNull(tableLoader, "tableLoader is required.");
      Preconditions.checkNotNull(splitAssignerFactory, "asignerFactory is required.");
      Preconditions.checkNotNull(bulkFormat, "bulkFormat is required.");
    }
  }
}

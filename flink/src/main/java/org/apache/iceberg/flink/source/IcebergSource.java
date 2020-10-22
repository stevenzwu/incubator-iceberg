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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableInfo;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.AssignerState;
import org.apache.iceberg.flink.source.enumerator.ContinuousEnumSettings;
import org.apache.iceberg.flink.source.enumerator.AbstractEnumState;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumStateSerializer;
import org.apache.iceberg.flink.source.assigner.Assigner;
import org.apache.iceberg.flink.source.assigner.SimpleAssigner;
import org.apache.iceberg.flink.source.enumerator.AbstractEnumerator;
import org.apache.iceberg.flink.source.reader.DataIteratorFactory;
import org.apache.iceberg.flink.source.reader.IcebergSourceReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;

@Experimental
public class IcebergSource<T, EnumStateT extends AbstractEnumState, AssignerStateT extends AssignerState>
    implements Source<T, IcebergSourceSplit, EnumStateT> {

  private final TableLoader tableLoader;
  private final ContinuousEnumSettings contEnumSettings;
  private final Assigner.Provider assignerFactory;
  private final ScanContext scanContext;
  private final DataIteratorFactory<T> iteratorFactory;

  private final TableInfo tableInfo;

  IcebergSource(
      TableLoader tableLoader,
      @Nullable ContinuousEnumSettings contEnumSettings,
      Assigner.Provider assignerFactory,
      ScanContext scanContext,
      DataIteratorFactory<T> iteratorFactory) {

    this.tableLoader = tableLoader;
    this.contEnumSettings = contEnumSettings;
    this.assignerFactory = assignerFactory;
    this.scanContext = scanContext;
    this.iteratorFactory = iteratorFactory;

    // extract serializable table info once during initialization
    this.tableInfo = TableInfo.fromTable(tableLoader.loadTable());
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
  public SourceReader createReader(SourceReaderContext readerContext) {
    FutureNotifier futureNotifier = new FutureNotifier();
    // This is unbounded queue right now.
    // Flink already has a fix that defaults capacity to 2
    // and makes it configurable via constructor arg.
    FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue =
        new FutureCompletingBlockingQueue<>(futureNotifier);
    // we should be able to getConfiguration from SourceReaderContext in the future
    Configuration config = new Configuration();
    return new IcebergSourceReader(
        futureNotifier,
        elementsQueue,
        config,
        readerContext,
        tableInfo,
        scanContext,
        iteratorFactory);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, EnumStateT> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext) {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, EnumStateT> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext, EnumStateT enumState)
      throws IOException {
    return createEnumerator(enumContext, enumState);
  }

  @Override
  public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
    return IcebergSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<EnumStateT> getEnumeratorCheckpointSerializer() {
    return IcebergEnumStateSerializer.INSTANCE;
  }

  private SplitEnumerator<IcebergSourceSplit, EnumStateT> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      AbstractEnumState checkpoint) {

    tableLoader.open();
    final Table table = tableLoader.loadTable();
    final Assigner assigner = assignerFactory.create();

    if (contEnumSettings != null) {
      throw new UnsupportedOperationException("Continuous enumeration mode not supported yet");
    } else {
      if (checkpoint != null) {
        assigner.addSplits(checkpoint.getSplits());
      } else {
        final List<IcebergSourceSplit> splits = FlinkSplitGenerator
            .createIcebergSourceSplits(table, scanContext);
        assigner.addSplits(splits);
      }
      return new AbstractEnumerator(enumContext, assigner);
    }
  }

  @VisibleForTesting
  ScanContext scanContext() {
    return scanContext;
  }

  @VisibleForTesting
  TableInfo tableInfo() {
    return tableInfo;
  }

  public static <T> Builder<T> builder(TableLoader tableLoader) {
    return new Builder<>(tableLoader);
  }

  public static class Builder<T> {

    private final TableLoader tableLoader;

    private ContinuousEnumSettings contEnumSettings;
    private Assigner.Provider assignerFactory;
    private ScanContext scanContext;
    private DataIteratorFactory<T> iteratorFactory;

    Builder(TableLoader tableLoader) {
      this.tableLoader = tableLoader;
      this.assignerFactory = () -> new SimpleAssigner();
      this.scanContext = new ScanContext();
    }

    public Builder<T> continuousEnumSettings(ContinuousEnumSettings newContEnumSettings) {
      this.contEnumSettings = newContEnumSettings;
      return this;
    }

    public Builder<T> assignerFactory(Assigner.Provider newAssignerFactory) {
      this.assignerFactory = newAssignerFactory;
      return this;
    }

    public Builder<T> scanContext(ScanContext newScanContext) {
      this.scanContext = newScanContext;
      return this;
    }

    public Builder<T> iteratorFactory(DataIteratorFactory<T> newIteratorFactory) {
      this.iteratorFactory = newIteratorFactory;
      return this;
    }

    public IcebergSource build() {
      doSanityCheck();
      return new IcebergSource(
          tableLoader,
          contEnumSettings,
          assignerFactory,
          scanContext,
          iteratorFactory
      );
    }

    private void doSanityCheck() {
      Preconditions.checkNotNull(iteratorFactory, "iteratorFactory is required.");
    }
  }
}

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
import javax.annotation.Nullable;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.flink.TableInfo;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssigner;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerState;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerStateSerializer;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerFactory;
import org.apache.iceberg.flink.source.assigner.SplitAssignerState;
import org.apache.iceberg.flink.source.assigner.SplitAssignerStateSerializer;
import org.apache.iceberg.flink.source.enumerator.ContinuousEnumSettings;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumState;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumStateSerializer;
import org.apache.iceberg.flink.source.enumerator.StaticIcebergEnumerator;
import org.apache.iceberg.flink.source.reader.DataIteratorFactory;
import org.apache.iceberg.flink.source.reader.IcebergSourceReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;

@Experimental
public class IcebergSource<T,
    SplitAssignerStateT extends SplitAssignerState,
    SplitAssignerT extends SplitAssigner<SplitAssignerStateT>,
    SplitAssignerStateSerializerT extends SplitAssignerStateSerializer<SplitAssignerStateT>>
    implements Source<T, IcebergSourceSplit, IcebergEnumState<SplitAssignerStateT>> {

  private final Configuration config;
  private final TableLoader tableLoader;
  private final ContinuousEnumSettings contEnumSettings;
  private final ScanContext scanContext;
  private final DataIteratorFactory<T> iteratorFactory;
  private final SplitAssignerFactory<SplitAssignerStateT, SplitAssignerT,
      SplitAssignerStateSerializerT> assignerFactory;

  private final TableInfo tableInfo;

  IcebergSource(
      Configuration config,
      TableLoader tableLoader,
      @Nullable ContinuousEnumSettings contEnumSettings,
      ScanContext scanContext,
      DataIteratorFactory<T> iteratorFactory,
      SplitAssignerFactory<SplitAssignerStateT, SplitAssignerT,
          SplitAssignerStateSerializerT> assignerFactory) {

    this.config = config;
    this.tableLoader = tableLoader;
    this.contEnumSettings = contEnumSettings;
    this.scanContext = scanContext;
    this.iteratorFactory = iteratorFactory;
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
    FutureNotifier futureNotifier = new FutureNotifier();
    // This is unbounded queue right now.
    // Flink already has a fix that defaults capacity to 2
    // and makes it configurable via constructor arg.
    FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue =
        new FutureCompletingBlockingQueue<>(futureNotifier);
    // we should be able to getConfiguration from SourceReaderContext in the future
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
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumState<SplitAssignerStateT>> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext) {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumState<SplitAssignerStateT>> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext, IcebergEnumState<SplitAssignerStateT> enumState)
      throws IOException {
    return createEnumerator(enumContext, enumState);
  }

  @Override
  public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
    return IcebergSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<IcebergEnumState<SplitAssignerStateT>> getEnumeratorCheckpointSerializer() {
    SplitAssignerStateSerializer<SplitAssignerStateT> assignerSerializer = assignerFactory.createSerializer();
    return new IcebergEnumStateSerializer(assignerSerializer);
  }

  private SplitEnumerator<IcebergSourceSplit, IcebergEnumState<SplitAssignerStateT>> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      @Nullable IcebergEnumState<SplitAssignerStateT> enumState) {

    final SplitAssigner<SplitAssignerStateT> assigner;
    if (enumState == null) {
      assigner = assignerFactory.createAssigner();
    } else {
      assigner = assignerFactory.createAssigner(enumState.assignerState());
    }
    if (contEnumSettings != null) {
      throw new UnsupportedOperationException("Continuous enumeration mode not supported yet");
    } else {
      return new StaticIcebergEnumerator(
          enumContext,
          tableLoader,
          scanContext,
          assigner,
          enumState);
    }
  }

  public static <T> Builder<T, SimpleSplitAssignerState, SimpleSplitAssigner,
      SimpleSplitAssignerStateSerializer> useSimpleAssigner() {
    SimpleSplitAssignerFactory assignerFactory = new SimpleSplitAssignerFactory();
    return new Builder<>(assignerFactory);
  }

  public static class Builder<T,
      SplitAssignerStateT extends SplitAssignerState,
      SplitAssignerT extends SplitAssigner<SplitAssignerStateT>,
      SplitAssignerStateSerializerT extends SplitAssignerStateSerializer<SplitAssignerStateT>> {

    private final SplitAssignerFactory<SplitAssignerStateT, SplitAssignerT,
        SplitAssignerStateSerializerT> assignerFactory;

    // required
    private TableLoader tableLoader;
    private DataIteratorFactory<T> iteratorFactory;

    // optional
    private Configuration config;
    private ScanContext scanContext;
    @Nullable private ContinuousEnumSettings contEnumSettings;

    Builder(SplitAssignerFactory<SplitAssignerStateT, SplitAssignerT,
        SplitAssignerStateSerializerT> assignerFactory) {
      this.assignerFactory = assignerFactory;
      this.config = new Configuration();
      this.scanContext = new ScanContext();
    }

    public Builder<T, SplitAssignerStateT, SplitAssignerT,
        SplitAssignerStateSerializerT> tableLoader(TableLoader loader) {
      this.tableLoader = loader;
      return this;
    }

    public Builder<T, SplitAssignerStateT, SplitAssignerT,
        SplitAssignerStateSerializerT> iteratorFactory(DataIteratorFactory<T> newIteratorFactory) {
      this.iteratorFactory = newIteratorFactory;
      return this;
    }

    public Builder<T, SplitAssignerStateT, SplitAssignerT,
        SplitAssignerStateSerializerT> config(Configuration newConfig) {
      this.config = newConfig;
      return this;
    }

    public Builder<T, SplitAssignerStateT, SplitAssignerT,
        SplitAssignerStateSerializerT> scanContext(ScanContext newScanContext) {
      this.scanContext = newScanContext;
      return this;
    }

    public Builder<T, SplitAssignerStateT, SplitAssignerT,
        SplitAssignerStateSerializerT> continuousEnumSettings(ContinuousEnumSettings newContEnumSettings) {
      this.contEnumSettings = newContEnumSettings;
      return this;
    }

    public IcebergSource<T, SplitAssignerStateT, SplitAssignerT, SplitAssignerStateSerializerT> build() {
      checkRequired();
      return new IcebergSource(
          config,
          tableLoader,
          contEnumSettings,
          scanContext,
          iteratorFactory,
          assignerFactory);
    }

    private void checkRequired() {
      Preconditions.checkNotNull(tableLoader, "tableLoader is required.");
      Preconditions.checkNotNull(iteratorFactory, "iteratorFactory is required.");
    }
  }
}

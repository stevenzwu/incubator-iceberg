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
import org.apache.iceberg.flink.source.enumerator.ContinuousEnumSettings;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumState;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumStateSerializer;
import org.apache.iceberg.flink.source.enumerator.IcebergSplitAssigner;
import org.apache.iceberg.flink.source.enumerator.SimpleIcebergSplitAssigner;
import org.apache.iceberg.flink.source.enumerator.StaticIcebergSplitEnumerator;
import org.apache.iceberg.flink.source.reader.DataIteratorFactory;
import org.apache.iceberg.flink.source.reader.IcebergSourceReader;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;

@Experimental
public class IcebergSource<T> implements Source<T, IcebergSourceSplit, IcebergEnumState> {

  private final Configuration config;
  private final TableLoader tableLoader;
  private final ContinuousEnumSettings contEnumSettings;
  private final IcebergSplitAssigner.Provider assignerFactory;
  private final ScanContext scanContext;
  private final DataIteratorFactory<T> iteratorFactory;

  private final TableInfo tableInfo;

  IcebergSource(
      Configuration config,
      TableLoader tableLoader,
      @Nullable ContinuousEnumSettings contEnumSettings,
      IcebergSplitAssigner.Provider assignerFactory,
      ScanContext scanContext,
      DataIteratorFactory<T> iteratorFactory) {

    this.config = config;
    this.tableLoader = tableLoader;
    this.contEnumSettings = contEnumSettings;
    this.assignerFactory = assignerFactory;
    this.scanContext = scanContext;
    this.iteratorFactory = iteratorFactory;

    // extract serializable table info once during initialization
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      this.tableInfo = TableInfo.fromTable(tableLoader.loadTable());
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
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext) {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumState> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext, IcebergEnumState checkpoint)
      throws IOException {
    return createEnumerator(enumContext, checkpoint);
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
      IcebergEnumState checkpoint) {

    final IcebergSplitAssigner assigner = assignerFactory.create();
    if (contEnumSettings != null) {
      throw new UnsupportedOperationException("Continuous enumeration mode not supported yet");
    } else {
      if (checkpoint != null) {
        assigner.addSplits(checkpoint.getSplits());
      } else {
        tableLoader.open();
        try (TableLoader loader = tableLoader) {
          final Table table = loader.loadTable();
          final List<IcebergSourceSplit> splits = FlinkSplitGenerator
              .planIcebergSourceSplits(table, scanContext);
          assigner.addSplits(splits);
        } catch (IOException e) {
          throw new RuntimeException("failed to close table loader", e);
        }
      }
      return new StaticIcebergSplitEnumerator(enumContext, assigner);
    }
  }

  @VisibleForTesting
  ScanContext scanContext() {
    return scanContext;
  }

  public static <T> Builder<T> builder(TableLoader tableLoader) {
    return new Builder<>(tableLoader);
  }

  public static class Builder<T> {

    private final TableLoader tableLoader;

    // required
    private DataIteratorFactory<T> iteratorFactory;

    // optional
    private Configuration config;
    private IcebergSplitAssigner.Provider assignerFactory;
    private ScanContext scanContext;
    @Nullable private ContinuousEnumSettings contEnumSettings;

    Builder(TableLoader tableLoader) {
      this.tableLoader = tableLoader;
      this.config = new Configuration();
      this.assignerFactory = () -> new SimpleIcebergSplitAssigner();
      this.scanContext = new ScanContext();
    }

    public Builder<T> iteratorFactory(DataIteratorFactory<T> newIteratorFactory) {
      this.iteratorFactory = newIteratorFactory;
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

    public Builder<T> assignerFactory(IcebergSplitAssigner.Provider newAssignerFactory) {
      this.assignerFactory = newAssignerFactory;
      return this;
    }

    public Builder<T> continuousEnumSettings(ContinuousEnumSettings newContEnumSettings) {
      this.contEnumSettings = newContEnumSettings;
      return this;
    }

    public IcebergSource<T> build() {
      doSanityCheck();
      return new IcebergSource(
          config,
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
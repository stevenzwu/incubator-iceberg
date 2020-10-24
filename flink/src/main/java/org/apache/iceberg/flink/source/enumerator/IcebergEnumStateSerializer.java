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

package org.apache.iceberg.flink.source.enumerator;

import java.io.IOException;
import java.util.Optional;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.flink.source.assigner.SplitAssignerState;
import org.apache.iceberg.flink.source.assigner.SplitAssignerStateSerializer;

public class IcebergEnumStateSerializer<
    SplitAssignerStateT extends SplitAssignerState,
    SplitAssignerStateSerializerT extends SplitAssignerStateSerializer<SplitAssignerStateT>>
    implements SimpleVersionedSerializer<IcebergEnumState<SplitAssignerStateT>> {

  private static final int VERSION = 1;

  private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
      ThreadLocal.withInitial(() -> new DataOutputSerializer(1024));

  private final SplitAssignerStateSerializerT assignerSerializer;

  public IcebergEnumStateSerializer(SplitAssignerStateSerializerT assignerSerializer) {
    this.assignerSerializer = assignerSerializer;
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergEnumState<SplitAssignerStateT> enumState) throws IOException {
    final DataOutputSerializer out = SERIALIZER_CACHE.get();
    // serialize enumerator state
    Optional<Long> snapshotIdOpt = enumState.lastEnumeratedSnapshotId();
    out.writeBoolean(snapshotIdOpt.isPresent());
    if (snapshotIdOpt.isPresent()) {
      out.writeLong(snapshotIdOpt.get());
    }
    // serialize pluggable assigner state
    out.writeInt(assignerSerializer.getVersion());
    assignerSerializer.serialize(enumState.assignerState(), out);
    final byte[] result = out.getCopyOfBuffer();
    out.clear();
    return result;
  }

  @Override
  public IcebergEnumState<SplitAssignerStateT> deserialize(int version, byte[] serialized) throws IOException {
    if (version == 1) {
      return deserializeV1(serialized);
    }
    throw new IOException("Unknown version: " + version);
  }

  private IcebergEnumState<SplitAssignerStateT> deserializeV1(byte[] serialized) throws IOException {
    final DataInputDeserializer in = new DataInputDeserializer(serialized);
    // deserialize enumerator state
    final boolean hasSnapshotId = in.readBoolean();
    final Optional<Long> snapshotIdOpt;
    if (hasSnapshotId) {
      snapshotIdOpt = Optional.of(in.readLong());
    } else {
      snapshotIdOpt = Optional.empty();
    }
    // deserialize assigner state
    final int assignerSerializerVersion = in.readInt();
    SplitAssignerStateT assignerState = assignerSerializer
        .deserialize(assignerSerializerVersion, in);
    return new IcebergEnumState<>(snapshotIdOpt, assignerState);
  }
}

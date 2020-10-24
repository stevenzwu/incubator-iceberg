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

package org.apache.iceberg.flink.source.assigner;

import java.io.IOException;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.InstantiationUtil;

/**
 * TODO: use Java serialization for now.
 * will need to write our own serializer before release.
 */
public class SimpleSplitAssignerStateSerializer implements
    SplitAssignerStateSerializer<SimpleSplitAssignerState> {

  public static final SimpleSplitAssignerStateSerializer INSTANCE = new SimpleSplitAssignerStateSerializer();

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public void serialize(SimpleSplitAssignerState state, DataOutputSerializer out) throws IOException {
    final byte[] assignerStateBytes = InstantiationUtil.serializeObject(state);
    out.writeInt(assignerStateBytes.length);
    out.write(assignerStateBytes);
  }

  @Override
  public SimpleSplitAssignerState deserialize(int version, DataInputDeserializer in) throws IOException {
    final int bytes = in.readInt();
    final byte[] assignerStateBytes = new byte[bytes];
    in.read(assignerStateBytes);
    try {
      return InstantiationUtil.deserializeObject(assignerStateBytes, getClass().getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize the split.", e);
    }
  }
}

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

import java.util.Collection;
import java.util.Collections;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SplitEnumerator;

/**
 * These source events aren't included in Flink 1.11.2.
 * Copying the definitions here.
 *
 * Once the latest flink-connector-base changes land in 1.11.3 or 1.12.0,
 * we can remove at least NoMoreSplitsEvent and NoSplitAvailableEvent.
 */
public class IcebergSourceEvents {

  /**
   * A {@link SourceEvent} representing the request for a split, typically sent from the
   * {@link SourceReader} to the {@link SplitEnumerator}.
   *
   * TODO: push change to Flink to carry the finished splitIds.
   * Certain ordering behavior requires tracking of finished splits.
   */
  public static final class SplitRequestEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;

    private final Collection<String> finishedSplitIds;

    public SplitRequestEvent() {
      this.finishedSplitIds = Collections.emptyList();
    }

    public SplitRequestEvent(Collection<String> finishedSplitIds) {
      this.finishedSplitIds = finishedSplitIds;
    }

    public Collection<String> finishedSplitIds() {
      return finishedSplitIds;
    }
  }

  /**
   * A source event sent from the SplitEnumerator to the SourceReader to indicate that no more
   * splits will be assigned to the source reader anymore. So once the SplitReader finishes
   * reading the currently assigned splits, they can exit.
   */
  public static class NoMoreSplitsEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;
  }

  /**
   * A simple {@link SourceEvent} indicating that there is no split available for the reader (any more).
   * This event is typically sent from the {@link SplitEnumerator} to the {@link SourceReader}.
   */
  public static final class NoSplitAvailableEvent implements SourceEvent {
    private static final long serialVersionUID = 1L;
  }

}
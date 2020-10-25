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

import java.io.Serializable;
import java.time.Duration;
import javax.annotation.Nullable;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Settings for continuous split enumeration
 */
public class ContinuousEnumConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  public enum StartingStrategy {

    /**
     * First do a regular table scan.
     * then switch to incremental mode.
     */
    TABLE_SCAN_THEN_INCREMENTAL,

    /**
     * Start incremental mode from latest snapshot
     */
    LATEST_SNAPSHOT,

    /**
     * Start incremental mode from earliest snapshot
     */
    EARLIEST_SNAPSHOT,

    /**
     * Start incremental mode from a specific startSnapshotId
     */
    SPECIFIC_START_SNAPSHOT_ID,

    /**
     * Start incremental mode from a specific startTimestamp
     */
    SPECIFIC_START_SNAPSHOT_TIMESTAMP
  }

  private final Duration discoveryInterval;
  private final StartingStrategy startingStrategy;
  @Nullable private final Long startSnapshotId;
  @Nullable private final Long startSnapshotTimeMs;

  private ContinuousEnumConfig(Builder builder) {
    this.discoveryInterval = builder.discoveryInterval;
    this.startingStrategy = builder.startingStrategy;
    this.startSnapshotId = builder.startSnapshotId;
    this.startSnapshotTimeMs = builder.startSnapshotTimeMs;
  }

  public Duration discoveryInterval() {
    return discoveryInterval;
  }

  public StartingStrategy startingStrategy() {
    return startingStrategy;
  }

  public Long startSnapshotId() {
    return startSnapshotId;
  }

  public Long startSnapshotTimeMs() {
    return startSnapshotTimeMs;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("discoveryInterval", discoveryInterval)
        .add("startingStrategy", startingStrategy)
        .add("startSnapshotId", startSnapshotId)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    // required
    private Duration discoveryInterval;
    // optional
    private StartingStrategy startingStrategy = StartingStrategy.LATEST_SNAPSHOT;
    private Long startSnapshotId;
    private Long startSnapshotTimeMs;

    private Builder() {
    }

    public Builder discoveryInterval(Duration interval) {
      this.discoveryInterval = interval;
      return this;
    }

    public Builder startingStrategy(StartingStrategy strategy) {
      this.startingStrategy = strategy;
      return this;
    }

    public Builder startSnapshotId(long startId) {
      this.startSnapshotId = startId;
      return this;
    }

    public Builder startSnapshotTimeMs(long startTimeMs) {
      this.startSnapshotTimeMs = startTimeMs;
      return this;
    }

    public ContinuousEnumConfig build() {
      checkRequired();
      return new ContinuousEnumConfig(this);
    }

    private void checkRequired() {
      Preconditions.checkNotNull(discoveryInterval,
          "discoveryInterval can't be null");
      switch (startingStrategy) {
        case SPECIFIC_START_SNAPSHOT_ID:
          Preconditions.checkNotNull(startSnapshotId,
              "Must set startSnapshotId with starting strategy: " + startingStrategy);
          break;
        case SPECIFIC_START_SNAPSHOT_TIMESTAMP:
          Preconditions.checkNotNull(startSnapshotTimeMs,
              "Must set startSnapshotTimeMs with starting strategy: " + startingStrategy);
          break;
        default:
          break;
      }
    }
  }
}

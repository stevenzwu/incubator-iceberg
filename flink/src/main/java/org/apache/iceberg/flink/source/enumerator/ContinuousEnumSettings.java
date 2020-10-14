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
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * Settings describing how to do continuous split enumeration
 * for the continuous streaming mode.
 */
public class ContinuousEnumSettings implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Duration discoveryInterval;

  public ContinuousEnumSettings(Duration discoveryInterval) {
    this.discoveryInterval = Preconditions.checkNotNull(discoveryInterval,
        "discoveryInterval can't be null");
  }

  public Duration getDiscoveryInterval() {
    return discoveryInterval;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("discoveryInterval", discoveryInterval)
        .toString();
  }
}

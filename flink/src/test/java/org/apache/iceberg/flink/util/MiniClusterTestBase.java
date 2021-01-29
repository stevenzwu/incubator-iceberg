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

package org.apache.iceberg.flink.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

/**
 * This class is similar to {@link AbstractTestBase} with the difference
 * of disabling {@link CoreOptions#CHECK_LEAKED_CLASSLOADER},
 * because Flink tests can still use Flink's classloader to
 * read and validate table result after the Flink job is terminated.
 */
public class MiniClusterTestBase extends TestBaseUtils {

  private static final int DEFAULT_PARALLELISM = 4;

  private static final Configuration config = new Configuration()
      // disable classloader check as Avro may cache class/object in the serializers.
      .set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

  @ClassRule
  public static MiniClusterWithClientResource miniClusterResource = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
          .setNumberTaskManagers(1)
          .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
          .setConfiguration(config)
          .build());

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
}

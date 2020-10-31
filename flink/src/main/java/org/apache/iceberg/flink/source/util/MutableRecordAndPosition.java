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

package org.apache.iceberg.flink.source.util;

public class MutableRecordAndPosition<E> extends RecordAndPosition<E> {

  /**
   * Updates the record and position in this object.
   */
  public void set(E record, long offset, long recordSkipCount) {
    setRecord(record);
    setOffset(offset);
    setRecordSkipCount(recordSkipCount);
  }

  /**
   * Sets the position without setting a record.
   */
  public void setPosition(long offset, long recordSkipCount) {
    setOffset(offset);
    setRecordSkipCount(recordSkipCount);
  }

  /**
   * Sets the next record of a sequence. This increments the {@code recordSkipCount} by one.
   */
  public void setNext(E record) {
    setRecord(record);
    setRecordSkipCount(getRecordSkipCount() + 1);
  }
}

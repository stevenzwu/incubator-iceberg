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
import java.io.UncheckedIOException;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Base class of Flink iterators.
 *
 * @param <T> is the Java class returned by this iterator whose objects contain one or more rows.
 */
public class CombinedScanTaskIterator<T> implements CloseableIterator<T> {

  private final FileIteratorReader<T> fileIteratorReader;
  private final DecryptedInputFiles decryptedInputFiles;
  private final Iterator<FileScanTask> tasks;
  private final Position position;

  private CloseableIterator<T> currentIterator;

  public CombinedScanTaskIterator(CombinedScanTask combinedTask, FileIO io, EncryptionManager encryption,
                                  FileIteratorReader<T> fileIteratorReader) {
    this(combinedTask, io, encryption, fileIteratorReader, null);
  }

  public CombinedScanTaskIterator(CombinedScanTask combinedTask, FileIO io, EncryptionManager encryption,
                                  FileIteratorReader<T> fileIteratorReader, @Nullable Position startingPosition) {
    this.fileIteratorReader = fileIteratorReader;
    this.decryptedInputFiles = new DecryptedInputFiles(combinedTask, io, encryption);
    this.tasks = combinedTask.files().iterator();

    if (startingPosition != null) {
      this.position = startingPosition;
      // skip files
      Preconditions.checkArgument(position.fileOffset() < combinedTask.files().size(),
          String.format("Starting file offset is %d, while CombinedScanTask has %d files",
              position.fileOffset(), combinedTask.files().size()));
      for (long i = 0L; i < position.fileOffset; ++i) {
        tasks.next();
      }
    } else {
      this.position = new Position();
    }

    final FileScanTask startingFileTask = tasks.next();
    this.currentIterator = fileIteratorReader.open(startingFileTask, decryptedInputFiles);

    // skip records
    for (int i = 0; i < position.recordOffset(); ++i) {
      if (currentIterator.hasNext()) {
        currentIterator.next();
      } else {
        throw new IllegalArgumentException(String.format(
            "File has less than %d records: %s", position.recordOffset, startingFileTask.file().path()));
      }
    }
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    updateCurrentIterator();
    position.advanceRecord();
    return currentIterator.next();
  }

  public boolean isCurrentIteratorDone() {
    return !currentIterator.hasNext();
  }

  /**
   * Updates the current iterator field to ensure that the current Iterator is not exhausted.
   */
  private void updateCurrentIterator() {
    try {
      while (!currentIterator.hasNext() && tasks.hasNext()) {
        currentIterator.close();
        currentIterator = fileIteratorReader.open(tasks.next(), decryptedInputFiles);
        position.advanceFile();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    // close the current iterator
    currentIterator.close();
  }

  public Position position() {
    return position;
  }

  public static class Position {

    private long fileOffset;
    private long recordOffset;

    Position() {
      this.fileOffset = 0L;
      this.recordOffset = 0L;
    }

    public Position(CheckpointedPosition checkpointedPosition) {
      this.fileOffset = checkpointedPosition.getOffset();
      this.recordOffset = checkpointedPosition.getRecordsAfterOffset();
    }

    void advanceFile() {
      this.fileOffset += 1;
      this.recordOffset = 0L;
    }

    void advanceRecord() {
      this.recordOffset += 1L;
    }

    public long fileOffset() {
      return fileOffset;
    }

    public long recordOffset() {
      return recordOffset;
    }
  }
}

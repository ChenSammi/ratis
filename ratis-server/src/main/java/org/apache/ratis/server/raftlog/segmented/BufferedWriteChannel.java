/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.raftlog.segmented;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedBiFunction;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Supplier;

/**
 * Provides a buffering layer in front of a FileChannel for writing.
 *
 * This class is NOT threadsafe.
 */
class BufferedWriteChannel implements Closeable {
  static BufferedWriteChannel open(File file, boolean append, ByteBuffer buffer,
      Supplier<CompletableFuture<Object>> flushFuture) throws IOException {
    final RandomAccessFile raf = new RandomAccessFile(file, "rw");
    final FileChannel fc = raf.getChannel();
    if (append) {
      fc.position(fc.size());
    } else {
      fc.truncate(0);
    }
    Preconditions.assertSame(fc.size(), fc.position(), "fc.position");
    return new BufferedWriteChannel(fc, buffer, flushFuture);
  }

  private final FileChannel fileChannel;
  private final ByteBuffer writeBuffer;
  private boolean forced = true;
  private final Supplier<CompletableFuture<Object>> flushFuture;

  BufferedWriteChannel(FileChannel fileChannel, ByteBuffer byteBuffer,
      Supplier<CompletableFuture<Object>> flushFuture) {
    this.fileChannel = fileChannel;
    this.writeBuffer = byteBuffer;
    this.flushFuture = flushFuture;
  }

  void write(byte[] b) throws IOException {
    int offset = 0;
    while (offset < b.length) {
      int toPut = Math.min(b.length - offset, writeBuffer.remaining());
      writeBuffer.put(b, offset, toPut);
      offset += toPut;
      if (writeBuffer.remaining() == 0) {
        flushBuffer();
      }
    }
  }

  void preallocateIfNecessary(long size, CheckedBiFunction<FileChannel, Long, Long, IOException> preallocate)
      throws IOException {
    final long outstanding = writeBuffer.position() + size;
    if (fileChannel.position() + outstanding > fileChannel.size()) {
      preallocate.apply(fileChannel, outstanding);
    }
  }

  /**
   * Write any data in the buffer to the file and force a
   * sync operation so that data is persisted to the disk.
   *
   * @throws IOException if the write or sync operation fails.
   */
  void flush() throws IOException {
    flushBuffer();
    if (!forced) {
      fileChannel.force(false);
      forced = true;
    }
  }

  CompletableFuture<Object> asyncFlush(ExecutorService executor, long index) throws IOException {
    flushBuffer();
    if (forced) {
      return CompletableFuture.completedFuture(null);
    }
    PriorityTask<Object> task = new PriorityTask(index, () -> fileChannelForce());
    final CompletableFuture<Object> f = CompletableFuture.supplyAsync(task, executor);
    task.setFuture(f);
    forced = true;
    return f;
  }

  private Void fileChannelForce() {
    try {
      fileChannel.force(false);
    } catch (IOException e) {
      LogSegment.LOG.error("Failed to flush channel", e);
      throw new CompletionException(e);
    }
    return null;
  }

  /**
   * Write any data in the buffer to the file.
   *
   * @throws IOException if the write fails.
   */
  private void flushBuffer() throws IOException {
    if (writeBuffer.position() == 0) {
      return; // nothing to flush
    }

    writeBuffer.flip();
    do {
      fileChannel.write(writeBuffer);
    } while (writeBuffer.hasRemaining());
    writeBuffer.clear();
    forced = false;
  }

  boolean isOpen() {
    return fileChannel.isOpen();
  }

  @Override
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void close() throws IOException {
    if (!isOpen()) {
      return;
    }

    try {
      Optional.ofNullable(flushFuture).ifPresent(f -> f.get());
      fileChannel.truncate(fileChannel.position());
    } finally {
      fileChannel.close();
    }
  }

  public interface WithPriority extends Comparable<WithPriority> {
    long priority();
    @Override
    default int compareTo(WithPriority o) {
      // Reverse comparison so higher priority comes first.
      return Long.compare(o.priority(), priority());
    }
  }

  static class PriorityTask<Void> implements Supplier<Void>, WithPriority {
    private final Long priority;
    private final Supplier<Void> supplier;
    private CompletableFuture<Void> future;

    public PriorityTask(Long priority, Supplier<Void> supplier) {
      this.priority = priority;
      this.supplier = supplier;
    }

    public void setFuture(CompletableFuture<Void> future) {
      this.future = future;
    }

    public CompletableFuture<Void> getFuture() {
      return this.future;
    }

    @Override
    public Void get() {
      return supplier.get();
    }

    @Override
    public long priority() {
      return priority;
    }
  }

  public static class RunnableComparator implements Comparator<Runnable> {

    @Override
    @SuppressWarnings("unchecked")
    public int compare(Runnable r1, Runnable r2) {
      // T might be AsyncSupply, UniApply, etc., but we want to
      // compare our original Runnables.
      return ((Comparable) unwrap(r1)).compareTo(unwrap(r2));
    }

    private Object unwrap(Runnable r) {
      try {
        Field field = r.getClass().getDeclaredField("fn");
        field.setAccessible(true);
        // NB: For performance-intensive contexts, you may want to
        // cache these in a ConcurrentHashMap<Class<?>, Field>.
        return field.get(r);
      } catch (IllegalAccessException | NoSuchFieldException e) {
        throw new IllegalArgumentException("Couldn't unwrap " + r, e);
      }
    }
  }
}

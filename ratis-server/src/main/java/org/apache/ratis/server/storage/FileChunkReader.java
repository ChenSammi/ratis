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
package org.apache.ratis.server.storage;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Optional;

/** Read {@link FileChunkProto}s from a file. */
public class FileChunkReader implements Closeable {
  private final FileInfo info;
  private final Path relativePath;
  private final InputStream in;
  private final MessageDigest digester;
  /** The offset position of the current chunk. */
  private long offset = 0;
  /** The index of the current chunk. */
  private int chunkIndex = 0;

  /**
   * Construct a reader from a file specified by the given {@link FileInfo}.
   *
   * @param info the information of the file.
   * @param directory the directory where the file is stored.
   * @throws IOException if it failed to open the file.
   */
  public FileChunkReader(FileInfo info, RaftStorageDirectory directory) throws IOException {
    this.info = info;
    this.relativePath = Optional.of(info.getPath())
        .filter(Path::isAbsolute)
        .map(p -> directory.getRoot().toPath().relativize(p))
        .orElse(info.getPath());
    final File f = info.getPath().toFile();
    if (info.getFileDigest() == null) {
      digester = MD5Hash.getDigester();
      this.in = new DigestInputStream(new FileInputStream(f), digester);
    } else {
      digester = null;
      this.in = new FileInputStream(f);
    }
  }

  /**
   * Read the next chunk.
   *
   * @param chunkMaxSize maximum chunk size
   * @return the chunk read from the file.
   * @throws IOException if it failed to read the file.
   */
  public FileChunkProto readFileChunk(int chunkMaxSize) throws IOException {
    final long remaining = info.getFileSize() - offset;
    final int chunkLength = remaining < chunkMaxSize ? (int) remaining : chunkMaxSize;
    final ByteString data = ByteString.readFrom(in, chunkLength);
    final ByteString fileDigest = ByteString.copyFrom(
            digester != null? digester.digest(): info.getFileDigest().getDigest());

    final FileChunkProto proto = FileChunkProto.newBuilder()
        .setFilename(relativePath.toString())
        .setOffset(offset)
        .setChunkIndex(chunkIndex)
        .setDone(offset + chunkLength == info.getFileSize())
        .setData(data)
        .setFileDigest(fileDigest)
        .build();
    chunkIndex++;
    offset += chunkLength;
    return proto;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass())
        + "{chunkIndex=" + chunkIndex
        + ", offset=" + offset
        + ", " + info + '}';
  }
}

/**
 * Licensed to the zk9131 under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
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

package com.github.zk1931.pulsed.tree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.Adler32;

/**
 * File Node.
 */
public class FileNode extends Node {
  public final byte[] data;
  public final long fileChecksum;

  public FileNode(String fullPath,
                  long version,
                  byte[] data) {
    super(fullPath, version);
    if (data == null) {
      this.data = new byte[0];
    } else {
      this.data = data.clone();
    }
    this.fileChecksum = calcChecksum();
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  @Override
  public long getChecksum() {
    return this.fileChecksum;
  }

  @Override
  public String getNodeName() {
    return "file";
  }

  private long calcChecksum() {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (DataOutputStream dout = new DataOutputStream(bout)) {
      dout.write(data);
      dout.writeLong(version);
      dout.writeBytes(fullPath);
      Adler32 adler = new Adler32();
      adler.update(bout.toByteArray());
      return adler.getValue();
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage());
    }
  }
}

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

package com.github.zk1931.pulsed;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.Adler32;

/**
 * Session File Node.
 */
public class SessionFileNode extends FileNode {
  final long sessionID;
  final long sessionFileChecksum;

  public SessionFileNode(String fullPath,
                         long version,
                         long sessionID,
                         byte[] data) {
    super(fullPath, version, data);
    this.sessionID = sessionID;
    this.sessionFileChecksum = calcChecksum();
  }

  @Override
  public String getNodeName() {
    return "session-file";
  }

  @Override
  public long getChecksum() {
    return this.sessionFileChecksum;
  }

  private long calcChecksum() {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (DataOutputStream dout = new DataOutputStream(bout)) {
      // The checksum of parent class.
      dout.writeLong(this.fileChecksum);
      dout.writeLong(this.sessionID);
      Adler32 adler = new Adler32();
      adler.update(bout.toByteArray());
      return adler.getValue();
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage());
    }
  }
}

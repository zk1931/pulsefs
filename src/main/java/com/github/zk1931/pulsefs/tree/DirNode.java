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

package com.github.zk1931.pulsefs.tree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.zip.Adler32;

/**
 * Directory Node.
 */
public class DirNode extends Node {

  public final Map<String, Node> children;
  public final long dirChecksum;

  public DirNode(String fullPath,
                 long version,
                 Map<String, Node> children) {
    super(fullPath, version);
    this.children = Collections.unmodifiableMap(children);
    this.dirChecksum = calcChecksum();
  }

  @Override
  public boolean isDirectory() {
    return true;
  }

  @Override
  public long getChecksum() {
    return this.dirChecksum;
  }

  @Override
  public String getNodeName() {
    return "dir";
  }

  private long calcChecksum() {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (DataOutputStream dout = new DataOutputStream(bout)) {
      dout.writeLong(version);
      dout.writeBytes(fullPath);
      for (Node child : children.values()) {
        dout.writeLong(child.getChecksum());
      }
      Adler32 adler = new Adler32();
      adler.update(bout.toByteArray());
      return adler.getValue();
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage());
    }
  }
}

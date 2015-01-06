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

package com.github.zk1931.pulsefs;

/**
 * PulseFS configuration.
 */
public final class PulseFSConfig {
  public static final String PULSEFS_ROOT = "/pulsefs";
  public static final String PULSEFS_SERVERS_PATH = PULSEFS_ROOT + "/servers";
  public static final String PULSEFS_SESSIONS_PATH = PULSEFS_ROOT + "/sessions";
  public static final String PULSEFS_SNAPSHOT_PATH = PULSEFS_ROOT + "/snapshot";

  private String serverId = null;
  private String joinPeer = null;
  private String logDir = null;
  private int sessionTimeout = 10;
  private int clientPort = 8080;

  public void setServerId(String server) {
    this.serverId = server;
  }

  public String getServerId() {
    return this.serverId;
  }

  public void setJoinPeer(String peer) {
    this.joinPeer = peer;
  }

  public String getJoinPeer() {
    return this.joinPeer;
  }

  public void setLogDir(String dir) {
    this.logDir = dir;
  }

  public String getLogDir() {
    return this.logDir;
  }

  public void setSessionTimeout(int timeout) {
    this.sessionTimeout = timeout;
  }

  public int getSessionTimeout() {
    return this.sessionTimeout;
  }

  public void setPort(int port) {
    this.clientPort = port;
  }

  public int getPort() {
    return this.clientPort;
  }
}

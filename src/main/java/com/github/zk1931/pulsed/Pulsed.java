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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import com.github.zk1931.jzab.PendingRequests;
import com.github.zk1931.jzab.StateMachine;
import com.github.zk1931.jzab.Zab;
import com.github.zk1931.jzab.ZabConfig;
import com.github.zk1931.jzab.ZabException;
import com.github.zk1931.jzab.ZabException.NotBroadcastingPhase;
import com.github.zk1931.jzab.ZabException.TooManyPendingRequests;
import com.github.zk1931.jzab.Zxid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State machine.
 */
public final class Pulsed {
  private static final Logger LOG = LoggerFactory.getLogger(Pulsed.class);
  private final Zab zab;
  private String serverId;
  private final PulsedStateMachine stateMachine = new PulsedStateMachine();
  private Set<String> activeMembers;
  private Set<String> clusterMembers;
  private String leader;

  public Pulsed(String serverId, String joinPeer, String logDir) {
    this.serverId = serverId;
    if (this.serverId != null && joinPeer == null) {
      joinPeer = this.serverId;
    }
    ZabConfig config = new ZabConfig();
    if (logDir == null) {
      config.setLogDir(this.serverId);
    }
    if (joinPeer != null) {
      zab = new Zab(stateMachine, config, serverId, joinPeer);
    } else {
      // Recovers from log directory.
      zab = new Zab(stateMachine, config);
    }
    this.serverId = zab.getServerId();
  }

  public boolean isLeader() {
    return this.leader.equals(this.serverId);
  }

  public Set<String> getActiveMembers() {
    return this.activeMembers;
  }

  public Set<String> getClusterMembers() {
    return this.clusterMembers;
  }

  public String getLeader() {
    return this.leader;
  }

  public void removePeer(String peerId, Object ctx) throws ZabException {
    this.zab.remove(peerId, ctx);
  }

  public DataTree getTree() {
    return this.stateMachine.tree;
  }

  public void proposeStateChange(Command cmd, AsyncContext ctx)
      throws NotBroadcastingPhase, IOException, TooManyPendingRequests {
    ByteBuffer bb = Serializer.serialize(cmd);
    zab.send(bb, ctx);
  }

  /**
   * State machine of Pulsed.
   */
  class PulsedStateMachine implements StateMachine {

    final DataTree tree = new DataTree();

    @Override
    public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
      return message;
    }

    @Override
    public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                        Object ctx) {
      Command command = Serializer.deserialize(stateUpdate);
      if (ctx != null) {
        command.executeAndReply(this.tree, ctx);
      } else {
        try {
          command.execute(this.tree);
        } catch (DataTree.TreeException ex) {
          LOG.trace("exception ", ex);
        }
      }
    }

    @Override
    public void removed(String peerId, Object ctx) {
      AsyncContext context = (AsyncContext)ctx;
      HttpServletResponse response =
        (HttpServletResponse)(context.getResponse());
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      context.complete();
    }

    @Override
    public void flushed(ByteBuffer request, Object ctx) {
    }

    @Override
    public void save(OutputStream os) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void restore(InputStream is) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void snapshotDone(String fileName, Object ctx) {}

    @Override
    public void recovering(PendingRequests pendingRequests) {
      LOG.info("Recovering");
    }

    @Override
    public void leading(Set<String> activePeers,
                        Set<String> clusterConfig) {
      LOG.info("Leading with active members : {}, cluster members :{}",
                activePeers, clusterConfig);
      leader = serverId;
      activeMembers = activePeers;
      clusterMembers = clusterConfig;
    }

    @Override
    public void following(String leaderId, Set<String> clusterConfig) {
      LOG.info("Following with leader {}, cluster members :{}",
                leaderId, clusterConfig);
      leader = leaderId;
      clusterMembers = clusterConfig;
    }
  }
}

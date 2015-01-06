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

import com.github.zk1931.jzab.PendingRequests;
import com.github.zk1931.jzab.StateMachine;
import com.github.zk1931.jzab.Zab;
import com.github.zk1931.jzab.ZabConfig;
import com.github.zk1931.jzab.ZabException;
import com.github.zk1931.jzab.ZabException.NotBroadcastingPhase;
import com.github.zk1931.jzab.ZabException.TooManyPendingRequests;
import com.github.zk1931.jzab.Zxid;
import com.github.zk1931.pulsefs.tree.DataTree;
import com.github.zk1931.pulsefs.tree.DirNode;
import com.github.zk1931.pulsefs.tree.Node;
import com.github.zk1931.pulsefs.tree.SessionFileNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Set;
import javax.servlet.AsyncContext;

/**
 * State machine.
 */
public final class PulseFS {
  private static final Logger LOG = LoggerFactory.getLogger(PulseFS.class);
  private final Zab zab;
  private String serverId;
  private final PulseFSStateMachine stateMachine = new PulseFSStateMachine();
  private Set<String> activeMembers;
  private Set<String> clusterMembers;
  private String leader;
  private boolean isBroadcasting = false;
  private final PulseFSConfig config;
  private final ZabConfig zabConfig;

  private ExecutorService fixedPool = Executors.newFixedThreadPool(1);
  // transient state
  private DelayQueue<Session> ownedSessions = new DelayQueue<>();
  private Future<Void> terminatorFuture;

  private class Terminator implements Callable<Void> {
    private final DelayQueue<Session> sessionQueue;

    public Terminator(DelayQueue<Session> sessionQueue) {
      this.sessionQueue = sessionQueue;
    }

    public Void call() throws Exception {
      while (true) {
        Session session = this.sessionQueue.take();
        LOG.debug("Expiring session {}", session.sessionID);
        Command expire = new ExpireSessionCommand(session.sessionID);
        proposeStateChange(expire, null);
      }
    }
  }

  public PulseFS(PulseFSConfig config) {
    this.config = config;
    this.serverId = config.getServerId();
    if (this.serverId != null && config.getJoinPeer() == null) {
      config.setJoinPeer(this.serverId);
    }
    this.zabConfig = new ZabConfig();
    if (config.getLogDir() == null) {
      config.setLogDir(this.serverId);
    }
    zabConfig.setLogDir(config.getLogDir());
    if (config.getJoinPeer() != null) {
      zab = new Zab(stateMachine, zabConfig, serverId, config.getJoinPeer());
    } else {
      // Recovers from log directory.
      zab = new Zab(stateMachine, zabConfig);
    }
    this.serverId = zab.getServerId();
    terminatorFuture = fixedPool.submit(new Terminator(ownedSessions));
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

  public String getServerId() {
    return this.serverId;
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

  public void proposeFlushRequest(Command cmd, AsyncContext ctx)
      throws NotBroadcastingPhase, IOException, TooManyPendingRequests {
    ByteBuffer bb = Serializer.serialize(cmd);
    zab.flush(bb, ctx);
  }

  public void manageSession(long sessionID) {
    Session session = new Session(sessionID, config.getSessionTimeout());
    LOG.debug("Add session {}", session);
    this.ownedSessions.remove(session);
    this.ownedSessions.add(session);
  }

  public void abandonSession(long sessionID) {
    Session session = new Session(sessionID, config.getSessionTimeout());
    LOG.debug("Abandon session {}", session);
    this.ownedSessions.remove(session);
  }

  public void renewSession(long sessionID) {
    Session session = new Session(sessionID, config.getSessionTimeout());
    LOG.debug("Renew session {}", session);
    this.ownedSessions.remove(session);
    this.ownedSessions.add(session);
  }

  public boolean inWorkingState() {
    return this.isBroadcasting;
  }

  /**
   * State machine of PulseFS.
   */
  class PulseFSStateMachine implements StateMachine {

    final DataTree tree = new DataTree();

    PulseFSStateMachine() {
      try {
        // Initialzes special directories /pulsefs
        tree.createDir(PulseFSConfig.PULSEFS_ROOT, false);
        tree.createDir(PulseFSConfig.PULSEFS_SERVERS_PATH, false);
        tree.createDir(PulseFSConfig.PULSEFS_SESSIONS_PATH, false);
      } catch (DataTree.TreeException ex) {
        LOG.error("Exception ", ex);
        throw new RuntimeException(ex);
      }
    }

    @Override
    public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
      return message;
    }

    @Override
    public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                        Object ctx) {
      Command command = Serializer.deserialize(stateUpdate);
      if (ctx != null) {
        command.executeAndReply(PulseFS.this, ctx);
      } else {
        try {
          command.execute(PulseFS.this);
        } catch (DataTree.TreeException ex) {
          LOG.trace("exception ", ex);
        }
      }
    }

    @Override
    public void removed(String peerId, Object ctx) {
      RemoveCommand cmd = (RemoveCommand)ctx;
      cmd.executeAndReply(PulseFS.this, null);
    }

    @Override
    public void flushed(ByteBuffer request, Object ctx) {
      Command command = Serializer.deserialize(request);
      command.executeAndReply(PulseFS.this, ctx);
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
      isBroadcasting = false;
    }

    @Override
    public void leading(Set<String> activePeers,
                        Set<String> clusterConfig) {
      LOG.info("Leading with active members : {}, cluster members :{}",
                activePeers, clusterConfig);
      leader = serverId;
      activeMembers = activePeers;
      clusterMembers = clusterConfig;
      // Writes the new configuration to DataTree.
      writeConfigToTree();
      // The leader also needs the take the ownership of all the sessions.
      manageAllSessions();
      isBroadcasting = true;
    }

    @Override
    public void following(String leaderId, Set<String> clusterConfig) {
      LOG.info("Following with leader {}, cluster members :{}",
                leaderId, clusterConfig);
      leader = leaderId;
      clusterMembers = clusterConfig;
      isBroadcasting = true;
    }

    private void writeConfigToTree() {
      Command clusterCmd = new ClusterChangeCommand(clusterMembers,
                                                    activeMembers,
                                                    serverId);
      try {
        ByteBuffer bb = Serializer.serialize(clusterCmd);
        zab.send(bb, null);
      } catch (IOException | ZabException ex) {
        LOG.error("Exception : ", ex);
      }
    }

    private void manageAllSessions() {
      Node sessionDir;
      try {
        sessionDir = tree.getNode(config.PULSEFS_SESSIONS_PATH);
      } catch (DataTree.TreeException ex) {
        LOG.error("Caught exception while accessing sessions", ex);
        throw new RuntimeException(ex);
      }
      if (!(sessionDir instanceof DirNode)) {
        throw new RuntimeException("Wrong node type.");
      }
      try {
        for (Node node : ((DirNode)sessionDir).children.values()) {
          long sessionID = ((SessionFileNode)node).sessionID;
          Command manage = new ManageSessionCommand(sessionID, serverId);
          ByteBuffer bb = Serializer.serialize(manage);
          zab.send(bb, null);
        }
      } catch (IOException | ZabException ex) {
        LOG.error("Exception : ", ex);
      }
    }
  }
}

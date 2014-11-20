package com.github.zk1931.pulsed;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import com.github.zk1931.jzab.PendingRequests;
import com.github.zk1931.jzab.StateMachine;
import com.github.zk1931.jzab.Zab;
import com.github.zk1931.jzab.ZabConfig;
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

  /**
   * State machine of Pulsed.
   */
  static class PulsedStateMachine implements StateMachine {
    @Override
    public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
      return message;
    }

    @Override
    public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                        Object ctx) {
    }

    @Override
    public void removed(String peerId, Object ctx) {
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
    }

    @Override
    public void leading(Set<String> activeFollowers,
                        Set<String> clusterConfig) {
      LOG.debug("Leading {}", activeFollowers);
    }

    @Override
    public void following(String leader, Set<String> clusterConfig) {
      LOG.debug("Following {}", leader);
    }
  }
}

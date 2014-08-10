package com.github.zk1931.pulsed;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import org.apache.zab.QuorumZab;
import org.apache.zab.StateMachine;
import org.apache.zab.Zxid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * state machine.
 */
public final class Database implements StateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(Database.class);

  private QuorumZab zab;

  private String serverId;

  private LinkedBlockingQueue<AsyncContext> pending =
    new LinkedBlockingQueue<>();

  private ExecutorService fixedPool = Executors.newFixedThreadPool(1);

  private static class DelayedString implements Delayed,
                                                Comparable<Delayed> {
    final String string;
    final long delayNs;

    public DelayedString(String string, long delaySec) {
      this.string = string;
      this.delayNs = System.nanoTime() + delaySec * 1000 * 1000 * 1000;
    }

    @Override
    public int compareTo(Delayed that) {
      long diff = this.getDelay(TimeUnit.NANOSECONDS) -
                  that.getDelay(TimeUnit.NANOSECONDS);
      return (diff == 0) ? 0 : (diff < 0) ? -1 : 1;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delayNs - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public boolean equals(Object obj){
      if(obj instanceof DelayedString) {
        DelayedString that = (DelayedString)obj;
        return this.string.equals(that.string);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return string.hashCode();
    }
  }

  private static class Terminator implements Callable<Void> {
    private DelayQueue<DelayedString> memberQueue;

    public Terminator(DelayQueue<DelayedString> memberQueue) {
      this.memberQueue = memberQueue;
    }

    public Void call() throws Exception {
      while (true) {
        DelayedString expired = memberQueue.take();
        LOG.debug("Expired: {}", expired.string);
      }
    }
  }

  // transient state
  private DelayQueue<DelayedString> ownedMembers = new DelayQueue<>();
  private Future<Void> terminatorFuture;

  // persistent state
  private Map<String, Member> memberMap = new ConcurrentHashMap<>();

  public Database() {
    try {
      String selfId = System.getProperty("serverId");
      String logDir = System.getProperty("logdir");
      String joinPeer = System.getProperty("join");

      if (selfId != null && joinPeer == null) {
        joinPeer = selfId;
      }

      Properties prop = new Properties();
      if (selfId != null) {
        prop.setProperty("serverId", selfId);
        prop.setProperty("logdir", selfId);
      }
      if (joinPeer != null) {
        prop.setProperty("joinPeer", joinPeer);
      }
      if (logDir != null) {
        prop.setProperty("logdir", logDir);
      }
      zab = new QuorumZab(this, prop);
      this.serverId = zab.getServerId();
      terminatorFuture = fixedPool.submit(new Terminator(ownedMembers));
    } catch (IOException|InterruptedException ex) {
      LOG.error("Caught exception : ", ex);
      throw new RuntimeException();
    }
  }

  public void put(String member, String owner) {
    LOG.debug("Activating a session: member={}, owner={}", member, owner);
    if (owner.equals(this.serverId)) {
      DelayedString newTimeout = new DelayedString(member, 10);
      // This member might be alreadly in the queue if the client sends requests
      // concurrently.
      ownedMembers.remove(newTimeout);
      ownedMembers.add(newTimeout);
    }
    memberMap.put(member, new Member(member, owner, true));
  }

  /**
   * Add a request to this database.
   *
   * This method must be synchronized to ensure that the requests are sent to
   * Zab in the same order they get enqueued to the pending queue.
   */
  public synchronized boolean add(String member, AsyncContext context) {
    if (!pending.add(context)) {
      return false;
    }
    try {
      PutCommand command = new PutCommand(member, serverId);
      ByteBuffer bb = Serializer.serialize(command);
      zab.send(bb);
    } catch (IOException ex) {
      throw new RuntimeException();
    }
    return true;
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    return message;
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId) {
    LOG.debug("Received a message: {}", stateUpdate);
    Command command = Serializer.deserialize(stateUpdate);
    LOG.debug("Delivering a command: {} {}", zxid, command);
    command.execute(this);

    if (clientId == null || !clientId.equals(this.serverId)) {
      return;
    }

    AsyncContext context = pending.poll();
    if (context == null) {
      // There is no pending HTTP request to respond to.
      return;
    }
    HttpServletResponse response =
      (HttpServletResponse)(context.getResponse());
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    context.complete();
  }

  @Override
  public void getState(OutputStream os) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setState(InputStream is) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void recovering() {
    // If it's LOOKING state. Reply all pending request with 503 clear
    // pending queue.
    LOG.debug("Recovering");
    Iterator<AsyncContext> iter = pending.iterator();
    while (iter.hasNext()) {
      AsyncContext context = iter.next();
      HttpServletResponse response =
        (HttpServletResponse)(context.getResponse());
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      context.complete();
    }
    pending.clear();
  }

  @Override
  public void leading(Set<String> activeFollowers) {
    LOG.debug("Leading {}", activeFollowers);
  }

  @Override
  public void following(String leader) {
    LOG.debug("Following {}", leader);
  }

  @Override
  public void clusterChange(Set<String> members) {
    LOG.debug("Cluster changed: {}.", members);
  }

  public boolean touch(String member, int timeoutSec) {
    LOG.debug("Current owned members: {}", ownedMembers);
    DelayedString newTimeout = new DelayedString(member, timeoutSec);
    if (ownedMembers.remove(newTimeout)) {
      ownedMembers.add(newTimeout);
      LOG.debug("Session {} renewed for {} seconds", member, timeoutSec);
      return true;
    }
    return false;
  }
}

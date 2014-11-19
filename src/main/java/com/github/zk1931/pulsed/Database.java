package com.github.zk1931.pulsed;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import com.github.zk1931.jzab.PendingRequests;
import com.github.zk1931.jzab.StateMachine;
import com.github.zk1931.jzab.Zab;
import com.github.zk1931.jzab.ZabConfig;
import com.github.zk1931.jzab.ZabException;
import com.github.zk1931.jzab.Zxid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State machine.
 */
public final class Database implements StateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(Database.class);

  private Zab zab;

  private String serverId;

  private LinkedBlockingQueue<AsyncContext> pending =
    new LinkedBlockingQueue<>();

  private ExecutorService fixedPool = Executors.newFixedThreadPool(1);

  private class Terminator implements Callable<Void> {
    private DelayQueue<Member> memberQueue;

    public Terminator(DelayQueue<Member> memberQueue) {
      this.memberQueue = memberQueue;
    }

    public Void call() throws Exception {
      while (true) {
        String memberName = memberQueue.take().name;
        LOG.debug("Expired: {}", memberName);
        sendDeactivate(memberName);
      }
    }
  }

  // transient state
  private DelayQueue<Member> ownedMembers = new DelayQueue<>();
  private Future<Void> terminatorFuture;

  // persistent state
  private ConcurrentMap<String, Member> memberMap = new ConcurrentHashMap<>();
  private ConcurrentMap<String, Group> groupMap = new ConcurrentHashMap<>();

  public Database(String serverId, String joinPeer, String logDir) {
    this.serverId = serverId;
    if (this.serverId != null && joinPeer == null) {
      joinPeer = this.serverId;
    }
    ZabConfig config = new ZabConfig();
    if (logDir == null) {
      config.setLogDir(this.serverId);
    }
    if (joinPeer != null) {
      zab = new Zab(this, config, this.serverId, joinPeer);
    } else {
      // Recovers from log directory.
      zab = new Zab(this, config);
    }
    this.serverId = zab.getServerId();
    terminatorFuture = fixedPool.submit(new Terminator(ownedMembers));
  }

  public void put(String member, String owner) {
    LOG.debug("Activating a session: member={}, owner={}", member, owner);
    Member newMember = new Member(member, owner, true, 10);
    if (owner.equals(this.serverId)) {
      // This member might be alreadly in the queue if the client sends requests
      // concurrently.
      ownedMembers.remove(newMember);
      ownedMembers.add(newMember);
    }
    Member currentMember = memberMap.get(member);
    if (currentMember != null) {
      newMember.groups.addAll(currentMember.groups);
    }
    memberMap.put(member, newMember);
    for (String group : newMember.groups) {
      groupMap.get(group).members.put(member, newMember);
    }
    logMemberMap();
  }

  public void deactivate(String memberName, String owner) {
    LOG.debug("Deactivating a session: member={}", memberName);
    Member member = memberMap.get(memberName);
    if (member == null) {
      LOG.warn("Got a deactivate command for a non-existent member: {}",
               memberName);
      return;
    }
    if (!member.owner.equals(owner)) {
      LOG.debug("Ignoring deactivate {} command from {}. Current owner is {}",
                memberName, owner, member.owner);
      return;
    }
    Member newMember = new Member(member.name, member.owner, false, 10);
    newMember.groups.addAll(member.groups);
    ownedMembers.remove(newMember);
    if (!memberMap.replace(memberName, member, newMember)) {
      LOG.warn("Failed to update the member map for: {}", memberName);
      return;
    }
    for (String group : newMember.groups) {
      groupMap.get(group).members.put(memberName, newMember);
    }
    logMemberMap();
  }

  private void logMemberMap() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Current member map: {}", memberMap);
    }
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
      zab.send(bb, null);
    } catch (ZabException ex) {
      throw new RuntimeException();
    } catch (IOException ex) {
      throw new RuntimeException();
    }
    return true;
  }

  /**
   * Send a request to join a group.
   *
   * This method must be synchronized to ensure that the requests are sent to
   * Zab in the same order they get enqueued to the pending queue.
   */
  public synchronized boolean sendJoin(String member, String group,
                                       AsyncContext context) {
    if (!pending.add(context)) {
      return false;
    }
    try {
      JoinCommand command = new JoinCommand(member, group);
      ByteBuffer bb = Serializer.serialize(command);
      zab.send(bb, null);
    } catch (ZabException ex) {
      throw new RuntimeException();
    } catch (IOException ex) {
      throw new RuntimeException();
    }
    return true;
  }

  public void join(String member, String group) {
    LOG.debug("Joining a group: member={}, group={}", member, group);
    Group newGroup = new Group();
    Member newMember = memberMap.get(member);
    if (newMember == null) {
      newMember = new Member(member, serverId, false, 10);
      memberMap.put(member, newMember);
    }
    newMember.groups.add(group);
    newGroup.members.put(member, newMember);
    Group currentGroup = groupMap.putIfAbsent(group, newGroup);
    if (currentGroup != null) {
      currentGroup.members.put(member, newMember);
    }
  }

  /**
   * Send a request to deactivate a member.
   *
   * @param member member to deactivate
   */
  public boolean sendDeactivate(String member) {
    try {
      DeactivateCommand command = new DeactivateCommand(member, serverId);
      ByteBuffer bb = Serializer.serialize(command);
      zab.send(bb, null);
    } catch (ZabException ex) {
      throw new RuntimeException();
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
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                      Object ctx) {
    Command command = Serializer.deserialize(stateUpdate);
    LOG.debug("Delivering a command: {} {} {}", zxid, command, clientId);
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
  public void leading(Set<String> activeFollowers, Set<String> clusterConfig) {
    LOG.debug("Leading {}", activeFollowers);
  }

  @Override
  public void following(String leader, Set<String> clusterConfig) {
    LOG.debug("Following {}", leader);
  }

  public boolean touch(String member, int timeoutSec) {
    LOG.debug("Current owned members: {}", ownedMembers);
    Member newTimeout = new Member(member, timeoutSec);
    if (ownedMembers.remove(newTimeout)) {
      ownedMembers.add(newTimeout);
      LOG.debug("Session {} renewed for {} seconds", member, timeoutSec);
      return true;
    }
    return false;
  }

  public String getMembers() {
    GsonBuilder builder = new GsonBuilder();
    builder.excludeFieldsWithoutExposeAnnotation();
    Gson gson = builder.create();
    String json = gson.toJson(memberMap);
    LOG.debug("Listing members: {}", json);
    return json;
  }

  public String getGroups() {
    GsonBuilder builder = new GsonBuilder();
    builder.excludeFieldsWithoutExposeAnnotation();
    Gson gson = builder.create();
    String json = gson.toJson(groupMap);
    LOG.debug("Listing groups: {}", json);
    return json;
  }
}

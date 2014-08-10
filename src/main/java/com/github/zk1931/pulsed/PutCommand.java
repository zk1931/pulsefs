package com.github.zk1931.pulsed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to create a new session.
 */
public final class PutCommand implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PutCommand.class);
  private static final long serialVersionUID = 0L;

  private final String member;
  private final String owner;

  public PutCommand(String member, String owner) {
    this.member = member;
    this.owner = owner;
  }

  public void execute(Database db) {
    db.put(member, owner);
  }

  public ByteBuffer toByteBuffer() throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(this);
      oos.close();
      return ByteBuffer.wrap(bos.toByteArray());
    }
  }

  public static PutCommand fromByteBuffer(ByteBuffer bb) {
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      return (PutCommand)ois.readObject();
    } catch (ClassNotFoundException|IOException ex) {
      LOG.error("Failed to deserialize: {}", bb, ex);
      throw new RuntimeException("Failed to deserialize ByteBuffer");
    }
  }

  public String toString() {
    return String.format("Put command: member=%s, owner=%s", member, owner);
  }
}

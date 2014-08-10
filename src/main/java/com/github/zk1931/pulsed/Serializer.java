package com.github.zk1931.pulsed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to serialize / deserialize commands.
 */
public final class Serializer {
  private static final Logger LOG = LoggerFactory.getLogger(Serializer.class);

  /**
   * Disables constructor.
   */
  private Serializer() {
  }

  /**
   * Serializes a command to ByteBuffer.
   */
  public static ByteBuffer serialize(Command command) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(command);
      oos.close();
      return ByteBuffer.wrap(bos.toByteArray());
    }
  }

  /**
   * Deserializes a ByteBuffer to command.
   */
  public static Command deserialize(ByteBuffer bb) {
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes);
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInputStream ois = new ObjectInputStream(bis)) {
      return (Command)ois.readObject();
    } catch (ClassNotFoundException|IOException ex) {
      LOG.error("Failed to deserialize: {}", bb, ex);
      throw new RuntimeException("Failed to deserialize ByteBuffer");
    }
  }
}

package com.github.zk1931.pulsed;

import java.io.Serializable;

/**
 * Command interface.
 */
public abstract class Command implements Serializable {

  private static final long serialVersionUID = 0L;

  /**
   * Executes a command against the database.
   *
   * @param pd pd to execute this command against.
   */
  abstract void execute(Pulsed pd, Object ctx);
}

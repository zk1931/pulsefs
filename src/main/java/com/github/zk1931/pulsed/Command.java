package com.github.zk1931.pulsed;

import java.io.Serializable;

/**
 * Command interface.
 */
public abstract class Command implements Serializable {

  private static final long serialVersionUID = 0L;

  abstract void execute(DataTree tree) throws DataTree.TreeException;

  abstract void executeAndReply(DataTree tree, Object ctx);
}

package com.github.zk1931.pulsed;

/**
 * Command interface.
 */
interface Command {
  /**
   * Executes a command against the database.
   *
   * @param db database to execute this command against.
   */
  void execute(Database db);
}

package com.github.zk1931.pulsed;

/**
 * Command interface.
 */
interface Command {
  /**
   * Executes a command against the database.
   *
   * @param db database to execute this command against.
   * @param origin server that sent this command.
   */
  void execute(Database db, String origin);
}

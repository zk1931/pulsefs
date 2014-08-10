package com.github.zk1931.pulsed;

import org.apache.zab.Zxid;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zabkv starts here.
 */
public final class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  private Main() {
  }

  public static void main(String[] args) throws Exception {
    Zxid zxid = new Zxid(0, 0);
    int port = Integer.parseInt(args[0]);
    Server server = new Server(port);
    ServletHandler handler = new ServletHandler();
    server.setHandler(handler);

    // Handlers with the initialization order >= 0 get initialized on startup.
    // If you don't specify this, Zab doesn't get initialized until the first
    // request is received.
    handler.addServletWithMapping(RequestHandler.class, "/*").setInitOrder(0);
    server.start();
    server.join();
  }
}

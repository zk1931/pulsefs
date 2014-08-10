package com.github.zk1931.pulsed;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
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
    int port = Integer.parseInt(args[0]);
    Server server = new Server(port);
    ServletContextHandler context =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/members");
    server.setHandler(context);

    // Handlers with the initialization order >= 0 get initialized on startup.
    // If you don't specify this, Zab doesn't get initialized until the first
    // request is received.
    ServletHolder holder = new ServletHolder(new MembersHandler());
    holder.setInitOrder(0);
    context.addServlet(holder, "/*");
    server.start();
    server.join();
  }
}

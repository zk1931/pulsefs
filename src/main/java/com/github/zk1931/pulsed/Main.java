package com.github.zk1931.pulsed;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
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
    Database db = new Database();

    // handles "/members" requests
    ServletContextHandler membersContext =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    membersContext.setContextPath("/members");
    membersContext.addServlet(new ServletHolder(new MembersHandler(db)), "/*");

    // handles "/groups" requests
    ServletContextHandler groupsContext =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    groupsContext.setContextPath("/groups");
    groupsContext.addServlet(new ServletHolder(new GroupsHandler(db)), "/*");

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    contexts.setHandlers(new Handler[] {membersContext, groupsContext});
    server.setHandler(contexts);
    server.start();
    server.join();
  }
}

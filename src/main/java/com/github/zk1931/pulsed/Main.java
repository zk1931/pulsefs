package com.github.zk1931.pulsed;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
    // Options for command arguments.
    Options options = new Options();

    Option port = OptionBuilder.withArgName("port")
                               .hasArg(true)
                               .isRequired(true)
                               .withDescription("port number")
                               .create("port");

    Option addr = OptionBuilder.withArgName("addr")
                               .hasArg(true)
                               .withDescription("addr (ip:port) for Zab.")
                               .create("addr");

    Option join = OptionBuilder.withArgName("join")
                               .hasArg(true)
                               .withDescription("the addr of server to join.")
                               .create("join");

    Option dir = OptionBuilder.withArgName("dir")
                              .hasArg(true)
                              .withDescription("the directory for logs.")
                              .create("dir");

    Option help = OptionBuilder.withArgName("h")
                               .hasArg(false)
                               .withLongOpt("help")
                               .withDescription("print out usages.")
                               .create("h");

    options.addOption(port)
           .addOption(addr)
           .addOption(join)
           .addOption(dir)
           .addOption(help);

    CommandLineParser parser = new BasicParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("pulsed", options);
        return;
      }
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("pulsed", options);
      return;
    }

    Server server = new Server(Integer.parseInt(cmd.getOptionValue("port")));
    Database db = new Database(cmd.getOptionValue("addr"),
                               cmd.getOptionValue("join"),
                               cmd.getOptionValue("dir"));

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

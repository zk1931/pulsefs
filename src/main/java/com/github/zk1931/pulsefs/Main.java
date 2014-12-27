/**
 * Licensed to the zk9131 under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.zk1931.pulsefs;

import java.util.EnumSet;
import javax.servlet.DispatcherType;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * pulsefs starts here.
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
    String usage = "./bin/pulsefs \"-port port -addr addr [-join peer] " +
      "[-dir data_dir]\" or \"-port port -dir data_dir\"";

    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(usage, options);
        return;
      }
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(usage, options);
      return;
    }

    Server server = new Server(Integer.parseInt(cmd.getOptionValue("port")));
    // Suppress "Server" header in HTTP response.
    for(Connector y : server.getConnectors()) {
      for(ConnectionFactory x  : y.getConnectionFactories()) {
        if(x instanceof HttpConnectionFactory) {
          ((HttpConnectionFactory)x).getHttpConfiguration()
                                    .setSendServerVersion(false);
        }
      }
    }

    PulseFS fs = new PulseFS(cmd.getOptionValue("addr"),
                             cmd.getOptionValue("join"),
                             cmd.getOptionValue("dir"));

    SessionFilter sessionFilter = new SessionFilter(fs);
    FilterHolder filters = new FilterHolder(sessionFilter);

    ServletContextHandler pulsefs =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    pulsefs.setContextPath(PulseFSConfig.PULSEFS_ROOT);
    pulsefs.setAllowNullPathInfo(true);
    pulsefs.addServlet(new ServletHolder(new PulseFSHandler(fs)), "/*");
    pulsefs.addFilter(filters, "/", EnumSet.of(DispatcherType.REQUEST));

    ServletContextHandler servers =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    servers.setContextPath(PulseFSConfig.PULSEFS_SERVERS_PATH);
    // Redirects /pulsefs/servers to /pulsefs/servers/
    servers.setAllowNullPathInfo(true);
    servers.addServlet(new ServletHolder(new PulseFSServersHandler(fs)), "/*");
    servers.addFilter(filters, "/", EnumSet.of(DispatcherType.REQUEST));

    ServletContextHandler tree =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    tree.setContextPath("/");
    tree.addServlet(new ServletHolder(new TreeHandler(fs)), "/*");
    tree.addFilter(filters, "/", EnumSet.of(DispatcherType.REQUEST));

    ServletContextHandler sessions =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    sessions.setContextPath(PulseFSConfig.PULSEFS_SESSIONS_PATH);
    // Redirects /pulsefs/sessions to /pulsefs/sessions/
    sessions.setAllowNullPathInfo(true);
    sessions.addServlet(new ServletHolder(new PulseFSSessionsHandler(fs)),
                                          "/*");
    sessions.addFilter(filters, "/", EnumSet.of(DispatcherType.REQUEST));

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    contexts.setHandlers(new Handler[] {sessions, servers, pulsefs, tree});
    server.setHandler(contexts);
    server.start();
    server.join();
  }
}

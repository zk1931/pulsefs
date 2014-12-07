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
 * pulsed starts here.
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
    String usage = "./bin/pulsed \"-port port -addr addr [-join peer] " +
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

    Pulsed pd = new Pulsed(cmd.getOptionValue("addr"),
                           cmd.getOptionValue("join"),
                           cmd.getOptionValue("dir"));

    ServletContextHandler pulsed =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    pulsed.setContextPath("/pulsed");
    pulsed.addServlet(new ServletHolder(new PulsedHandler(pd)), "/*");

    ServletContextHandler servers =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    servers.setContextPath("/pulsed/servers");
    servers.addServlet(new ServletHolder(new PulsedServersHandler(pd)), "/*");

    ServletContextHandler tree =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    tree.setContextPath("/");
    tree.addServlet(new ServletHolder(new TreeHandler(pd)), "/*");

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    contexts.setHandlers(new Handler[] {servers, pulsed, tree});
    server.setHandler(contexts);
    server.start();
    server.join();
  }
}

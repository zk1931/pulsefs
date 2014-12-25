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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session Filter, rejects all the requests come from the expired/unexistent
 * sessions.
 */
public class SessionFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(SessionFilter.class);

  final Pulsed pd;
  final Pattern pattern = Pattern.compile("session=(\\d+)");

  public SessionFilter(Pulsed pd) {
    this.pd = pd;
  }

  @Override
  public void destroy() {}

  @Override
  public void doFilter(ServletRequest request,
                       ServletResponse response,
                       FilterChain chain) throws IOException, ServletException {
    if (request instanceof HttpServletRequest) {
      if (!(response instanceof HttpServletResponse)) {
        throw new ServletException("Not HttpServletResponse object");
      }
      HttpServletRequest httpReq = (HttpServletRequest)request;
      if (httpReq.getQueryString() != null) {
        Matcher matcher = pattern.matcher(httpReq.getQueryString());
        if (matcher.find()) {
          long session = Long.parseLong(matcher.group(1));
          String sessionPath =
            String.format("%s/%016d", PulsedConfig.PULSED_SESSIONS_PATH,
                                      session);
          if (!this.pd.getTree().exist(sessionPath)) {
            // Session doesn't exist, reject this request.
            LOG.debug("session {} doesn't exist, reject request.", session);
            Utils.replyPrecondFailed((HttpServletResponse)response,
                                      "Session " + session + " times out");
            return;
          }
        }
      }
      if (!pd.inWorkingState()) {
        Utils.replyServiceUnavailable((HttpServletResponse)response);
        return;
      }
    }
    chain.doFilter(request, response);
  }

  @Override
  public void init(FilterConfig filterConfig) {
  }
}

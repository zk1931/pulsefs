package com.github.zk1931.pulsed;

import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse; import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles "/groups".
 */
public final class GroupsHandler extends HttpServlet {
  private static final Logger LOG =
      LoggerFactory.getLogger(GroupsHandler.class);
  private static final long serialVersionUID = 0L;

  private Database db;

  public GroupsHandler(Database db) {
    this.db = db;
  }

  @Override
  protected void service(HttpServletRequest request,
                         HttpServletResponse response)
      throws ServletException, IOException {
    switch(request.getMethod()) {
    case "GET":
      handleGet(request, response);
      break;
    case "PUT":
      handlePut(request, response);
      break;
    default:
      LOG.warn("Unhandled HTTP method: {}", request.getMethod());
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.setContentLength(0);
    }
  }

  private void handleGet(HttpServletRequest request,
                         HttpServletResponse response)
      throws ServletException, IOException {
    LOG.debug("Context path: {}, request path: {}", request.getContextPath(),
                                                    request.getRequestURI());
    String key = request.getRequestURI().substring(
                 request.getContextPath().length() + 1);
    if (key.isEmpty()) {
      // TODO handle read requests and write requests in a same thread?
      String json = db.getGroups();
      response.setContentType("application/json");
      response.setContentLength(json.length());
      response.setStatus(HttpServletResponse.SC_OK);
      response.getOutputStream().print(json);
    } else {
      // bad request
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.setContentLength(0);
    }
  }

  private void handlePut(HttpServletRequest request,
                         HttpServletResponse response)
      throws ServletException, IOException {
    String[] parts = request.getRequestURI().split("/");
    if (parts.length != 5 || !parts[3].equals("members")) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.setContentLength(0);
      return;
    }
    String group = parts[2];
    String member = parts[4];
    LOG.debug("Adding {} to {}", member, group);
    AsyncContext context = request.startAsync(request, response);
    db.sendJoin(member, group, context);
  }

  /**
   * "Disables" serializable.
   */
  private void writeObject(java.io.ObjectOutputStream stream)
      throws IOException {
    throw new java.io.NotSerializableException(getClass().getName());
  }

  /**
   * "Disables" serializable.
   */
  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    throw new java.io.NotSerializableException(getClass().getName());
  }
}

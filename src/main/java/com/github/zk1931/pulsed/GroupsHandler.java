package com.github.zk1931.pulsed;

import java.io.IOException;
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
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentLength(0);
      break;
    default:
      LOG.warn("Unhandled HTTP method: {}", request.getMethod());
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.setContentLength(0);
    }
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

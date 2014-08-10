package com.github.zk1931.pulsed;

import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse; import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request handler.
 */
public final class RequestHandler extends HttpServlet {
  private static final Logger LOG =
      LoggerFactory.getLogger(RequestHandler.class);
  private static final long serialVersionUID = 0L;

  private Database db = new Database();

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // remove the leading slash from the request path and use that as the key.
    String key = request.getPathInfo().substring(1);
    if (db.touch(key, 10)) {
      // session renewed.
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentLength(0);
    } else {
      // currently this server doesn't manage this member.
      AsyncContext context = request.startAsync(request, response);
      db.add(key, context);
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

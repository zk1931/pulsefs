package com.github.zk1931.pulsed;

import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
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
    LOG.info("Got GET request for key {}", key);
    byte[] value;
    if (key.equals("")) {
      value = db.getAll();
    } else {
      value = db.get(key);
    }
    response.setContentType("text/html");
    if (value == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentLength(value.length);
      response.getOutputStream().write(value);
    }
  }

  @Override
  protected void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    AsyncContext context = request.startAsync(request, response);
    // remove the leading slash from the request path and use that as the key.
    String key = request.getPathInfo().substring(1);
    LOG.info("Got PUT request for key {}", key);
    int length = request.getContentLength();
    if (length < 0) {
      // Don't accept requests without content length.
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.setContentLength(0);
      return;
    }
    byte[] value = new byte[length];
    int bytesRead = request.getInputStream().read(value);
    if (bytesRead != length) {
      LOG.debug("lengths mismatch: content length: {}, bytes read: {}",
                length, bytesRead);
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.setContentLength(0);
      return;
    }
    PutCommand command = new PutCommand(key, value);
    db.add(command, context);
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

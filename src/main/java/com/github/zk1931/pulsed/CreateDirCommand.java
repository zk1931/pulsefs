package com.github.zk1931.pulsed;

import com.github.zk1931.pulsed.DataTree.InvalidPath;
import com.github.zk1931.pulsed.DataTree.NodeAlreadyExist;
import com.github.zk1931.pulsed.DataTree.NotDirectory;
import com.github.zk1931.pulsed.DataTree.PathNotExist;
import com.github.zk1931.pulsed.DataTree.TreeException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.AsyncContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command for creating directory.
 */
public class CreateDirCommand extends Command {

  private static final long serialVersionUID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(PutCommand.class);

  final String path;
  final boolean recursive;

  public CreateDirCommand(String path, boolean recursive) {
    this.path = path;
    this.recursive = recursive;
  }

  void execute(DataTree tree)
      throws NotDirectory, NodeAlreadyExist, PathNotExist, InvalidPath {
    tree.createDir(this.path, -1, this.recursive);
  }

  void executeAndReply(DataTree tree, Object ctx) {
    AsyncContext context = (AsyncContext)ctx;
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    try {
      execute(tree);
      Utils.replyOK(response, null, context);
    } catch (PathNotExist ex) {
      Utils.notFound(response, ex.getMessage(), context);
    }catch (TreeException ex) {
      Utils.badRequest(response, ex.getMessage(), context);
    }
  }
}

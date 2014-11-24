package com.github.zk1931.pulsed;

import com.github.zk1931.pulsed.DataTree.DirectoryNode;
import com.github.zk1931.pulsed.DataTree.InvalidPath;
import com.github.zk1931.pulsed.DataTree.NodeAlreadyExist;
import com.github.zk1931.pulsed.DataTree.NotDirectory;
import com.github.zk1931.pulsed.DataTree.PathNotExist;
import com.github.zk1931.pulsed.DataTree.TreeException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.AsyncContext;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command for creating sequential file.
 */
public class CreateSeqFileCommand extends Command {

  private static final long serialVersionUID = 0L;
  private static final Logger LOG = LoggerFactory.getLogger(PutCommand.class);

  final String dirPath;
  final boolean recursive;
  final byte[] data;

  public CreateSeqFileCommand(String dirPath, byte[] data, boolean recursive) {
    this.dirPath = dirPath;
    this.recursive = recursive;
    this.data = data.clone();
  }

  void execute(DataTree tree)
      throws PathNotExist, InvalidPath, DirectoryNode, NotDirectory,
             NodeAlreadyExist {
    Node node = tree.getNode(this.dirPath);
    if (!(node instanceof DirNode)) {
      throw new NotDirectory(node.fullPath + " is not directory.");
    }
    Map<String, Node> children = ((DirNode)node).children;
    long max = -1;
    for (Node child : children.values()) {
      // File format is like : 0000000001
      String name = PathUtils.name(child.fullPath);
      if (name.matches("\\d{19}")) {
        long id = Long.parseLong(name);
        if (id > max) {
          max = id;
        }
      }
    }
    long newID = max + 1;
    String fileName = String.format("%019d", newID);
    String path = PathUtils.concat(this.dirPath, fileName);
    tree.createFile(path, this.data, -1, recursive);
  }

  void executeAndReply(DataTree tree, Object ctx) {
    AsyncContext context = (AsyncContext)ctx;
    HttpServletResponse response = (HttpServletResponse)(context.getResponse());
    try {
      execute(tree);
      Utils.replyOK(response, null, context);
    } catch (PathNotExist ex) {
      Utils.notFound(response, ex.getMessage(), context);
    } catch (TreeException ex) {
      Utils.badRequest(response, ex.getMessage(), context);
    }
  }
}

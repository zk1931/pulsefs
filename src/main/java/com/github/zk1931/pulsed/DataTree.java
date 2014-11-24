package com.github.zk1931.pulsed;

import static com.github.zk1931.pulsed.PathUtils.concat;
import static com.github.zk1931.pulsed.PathUtils.head;
import static  com.github.zk1931.pulsed.PathUtils.tail;
import static com.github.zk1931.pulsed.PathUtils.trimRoot;
import static com.github.zk1931.pulsed.PathUtils.validatePath;
import static com.github.zk1931.pulsed.PathUtils.ROOT_PATH;
import static com.github.zk1931.pulsed.PathUtils.SEP;
import java.util.TreeMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In memory data tree.
 */
public class DataTree {

  DirNode root = null;

  private static final Logger LOG = LoggerFactory.getLogger(DataTree.class);

  /**
   * Constructor of DataTree.
   */
  public DataTree() {
    this.root = new DirNode(ROOT_PATH,
                            (long)0,
                            (long)-1,
                            new TreeMap<String, Node>());
  }

  /**
   * Checks if there a node in given path.
   *
   * @param path the path of node.
   * @return true if the path exists, false otherwise.
   */
  boolean exist(String path) {
    try {
      getNode(path);
      return true;
    } catch (InvalidPath | PathNotExist | NotDirectory ex) {
      return false;
    }
  }

  /**
   * Gets the number of nodes in tree.
   *
   * @return the number of nodes in tree.
   */
  public int size() {
    return size(this.root);
  }

  /**
   * Creates a node of file type in tree.
   *
   * @param path the path of node.
   * @param data the initial data of the node.
   * @param sessionID the ID of the session of node, -1 if it doesn't belong to
   * any sessions.
   * @throws NotAlreadyExist if this path has arealdy existed in tree.
   * @throws PathNotExist if the path of its parent doesn't exist in tree.
   * @throws InvalidPath if the path is invalid.
   * @throws NotDirectory if the path goes through a non-directory node.
   */
  public void createFile(String path,
                         byte[] data,
                         long sessionID,
                         boolean recursive)
      throws NotDirectory, NodeAlreadyExist, PathNotExist, InvalidPath {
    validatePath(path);
    path = trimRoot(path);
    this.root =
      createNode(this.root, path, data, sessionID, recursive, false);
  }

  /**
   * Creates a node of directory type in tree.
   *
   * @param path the path of node.
   * @param sessionID the ID of the session of node, -1 if it doesn't belong to
   * any sessions.
   * @throws NotAlreadyExist if this path has arealdy existed in tree.
   * @throws PathNotExist if the path of its parent doesn't exist in tree.
   * @throws InvalidPath if the path is invalid.
   * @throws NotDirectory if the path goes through a non-directory node.
   */
  public void createDir(String path,
                        long sessionID,
                        boolean recursive)
      throws NotDirectory, NodeAlreadyExist, PathNotExist, InvalidPath {
    validatePath(path);
    path = trimRoot(path);
    this.root =
      createNode(this.root, path, null, sessionID, recursive, true);
  }

  /**
   * Deletes a node in tree.
   *
   * @param path the path of node.
   * @param recursive deletes all nodes under this directory as needed.
   * @throws PathNotExist if the path doesn't exist in tree.
   * @throws DirectoryNotEmpty delete a non-empty directory while recursive
   * parameter is false.
   * @throws InvalidPath if the path is invalid.
   * @throws DeleteRootDir trying to delete root directory.
   * @throws NotDirectory if the path goes through a non-directory node.
   */
  public void deleteNode(String path,
                         boolean recursive)
      throws PathNotExist, DirectoryNotEmpty, InvalidPath, DeleteRootDir,
             NotDirectory {
    validatePath(path);
    path = trimRoot(path);
    if (path.equals("")) {
      throw new DeleteRootDir();
    }
    this.root = (DirNode)deleteNode(this.root, path, recursive);
  }

  /**
   * Returns a node of given path.
   *
   * @param path the path of node.
   * @return a Node in the given path.
   * @throws InvalidPath if the path is invalid.
   * @throws PathNotExist if the path doesn't exist in tree.
   * @throws NotDirectory if the path goes through a non-directory node.
   */
  public Node getNode(String path)
      throws InvalidPath, PathNotExist, NotDirectory {
    if (path.equals(ROOT_PATH)) {
      return this.root;
    }
    validatePath(path);
    path = trimRoot(path);
    DirNode temp = this.root;
    while (path.contains(SEP)) {
      String name = head(path);
      Node child = temp.children.get(name);
      if (child == null) {
        throw new PathNotExist(concat(temp.fullPath, path) + " doesn't exist.");
      }
      if (!(child instanceof DirNode)) {
        throw new NotDirectory(child.fullPath + " is not directory.");
      }
      temp = (DirNode)child;
      path = tail(path);
    }
    Node child = temp.children.get(path);
    if (child == null) {
      throw new PathNotExist(concat(temp.fullPath, path) + " doesn't exist.");
    }
    return child;
  }

  /**
   * Updates the data of the node.
   *
   * @param path the path of node.
   * @param data the new data for node.
   * @param version updates data if the version matches the version of the
   * node, if the version is -1 then we'll always update the data.
   * @return the new version of node.
   * @throws PathNotExist if the path doesn't exist in tree.
   * @throws InvalidPath if the path is invalid.
   * @throws VersionNotMatch if the version doesn't match version of the node.
   * @throws DirectoryNode can't store data on node of directory type.
   * @throws NotDirectory if the path goes through a non-directory node.
   */
  public long setData(String path, byte[] data, long version)
      throws PathNotExist, InvalidPath, VersionNotMatch, DirectoryNode,
             NotDirectory {
    validatePath(path);
    path = trimRoot(path);
    this.root = (DirNode)setData(this.root, path, data, version);
    return this.root.version;
  }

  DirNode createNode(DirNode curNode,
                     String path,
                     byte[] data,
                     long sessionID,
                     boolean recursive,
                     boolean dir)
      throws NotDirectory, NodeAlreadyExist, PathNotExist {
    Node newChild;
    DirNode newNode;
    String childName;
    // Means now the path is the name of last node.
    if (!path.contains(SEP)) {
      childName = path;
      Node child = curNode.children.get(childName);
      if (child != null) {
        throw new NodeAlreadyExist(child.fullPath + " already exists.");
      }
      String fullPath = concat(curNode.fullPath, childName);
      if (dir) {
        Map<String, Node> children = new TreeMap<String, Node>();
        newChild = new DirNode(fullPath, 0, sessionID, children);
      } else {
        newChild = new FileNode(fullPath, 0, sessionID, data);
      }
    } else {
      // There's still '/' in path, we'll continue recursion.
      childName = head(path);
      String nextPath = tail(path);
      Node child = curNode.children.get(childName);
      if (child == null) {
        if (!recursive) {
          throw new PathNotExist(concat(curNode.fullPath, childName) +
              " doesn't exist");
        }
        // Recursive creation, create a new intermediate node.
        child = new DirNode(concat(curNode.fullPath, childName),
                            0,
                            -1,
                            new TreeMap<String, Node>());
      }
      if (!(child instanceof DirNode)) {
        throw new NotDirectory(child.fullPath + " is not directory.");
      }
      newChild =
        createNode((DirNode)child, nextPath, data, sessionID, recursive, dir);
    }
    Map<String, Node> newChildren = new TreeMap<>(curNode.children);
    newChildren.put(childName, newChild);
    newNode = new DirNode(curNode.fullPath,
                          curNode.version + 1,
                          curNode.sessionID,
                          newChildren);
    return newNode;
  }

  Node deleteNode(Node curNode,
                  String path,
                  boolean recursive)
      throws PathNotExist, DirectoryNotEmpty, NotDirectory {
    if (path.equals("")) {
      if (curNode.isDirectory() &&
          !((DirNode)curNode).children.isEmpty() &&
          !recursive) {
        throw new DirectoryNotEmpty(curNode.fullPath + " is not empty.");
      }
      return null;
    }
    if (!(curNode instanceof DirNode)) {
      throw new NotDirectory(curNode.fullPath + " is not directory.");
    }
    Node newChild;
    DirNode newNode;
    String childName = head(path);
    String nextPath = tail(path);
    Node child = ((DirNode)curNode).children.get(childName);
    if (child == null) {
      throw new PathNotExist(concat(curNode.fullPath, path) +
          " doesn't exist");
    }
    newChild = deleteNode(child, nextPath, recursive);
    Map<String, Node> newChildren = new TreeMap<>(((DirNode)curNode).children);
    if (newChild != null) {
      newChildren.put(childName, newChild);
    } else {
      newChildren.remove(childName);
    }
    newNode = new DirNode(curNode.fullPath,
                          curNode.version + 1,
                          curNode.sessionID,
                          newChildren);
    return newNode;
  }

  Node setData(Node curNode,
               String path,
               byte[] data,
               long version)
      throws PathNotExist, VersionNotMatch, DirectoryNode, NotDirectory {
    if (path.equals("")) {
      if (version != -1 && curNode.version != version) {
        throw new VersionNotMatch("Version " + version +
            " doesn't match node version " + curNode.version);
      }
      if (curNode.isDirectory()) {
        throw new DirectoryNode(curNode.fullPath + " is directory.");
      }
      return new FileNode(curNode.fullPath,
                          curNode.version + 1,
                          curNode.sessionID,
                          data);
    }
    if (!(curNode instanceof DirNode)) {
      throw new NotDirectory(curNode.fullPath + " is not directory.");
    }
    Node newChild;
    DirNode newNode;
    String childName = head(path);
    String nextPath = tail(path);
    Node child = ((DirNode)curNode).children.get(childName);
    if (child == null) {
      throw new PathNotExist(concat(curNode.fullPath, childName) +
          " doesn't exist");
    }
    newChild = setData(child, nextPath, data, version);
    Map<String, Node> newChildren = new TreeMap<>(((DirNode)curNode).children);
    newChildren.put(childName, newChild);
    newNode = new DirNode(curNode.fullPath,
                          curNode.version + 1,
                          curNode.sessionID,
                          newChildren);
    return newNode;
  }

  public static int size(Node curNode) {
    if (!(curNode instanceof DirNode)) {
      return 1;
    }
    int sum = 1;
    DirNode dirNode = (DirNode)curNode;
    for (Node node : dirNode.children.values()) {
      sum += size(node);
    }
    return sum;
  }

  /**
   * Base class for all the tree exceptions.
   */
  public abstract static class TreeException extends Exception {
    TreeException(String desc) {
      super(desc);
    }

    TreeException() {}
  }

  /**
   * Exception for a non-existing path.
   */
  public static class PathNotExist extends TreeException {
    PathNotExist(String desc) {
      super(desc);
    }

    PathNotExist() {}
  }

  /**
   * Exception for node arealdy exist.
   */
  public static class NodeAlreadyExist extends TreeException {
    NodeAlreadyExist(String desc) {
      super(desc);
    }

    NodeAlreadyExist() {}
  }

  /**
   * Exception for unmatched version.
   */
  public static class VersionNotMatch extends TreeException {
    VersionNotMatch(String desc) {
      super(desc);
    }

    VersionNotMatch() {}
  }

  /**
   * Exception of node is not directory.
   */
  public static class NotDirectory extends TreeException {
    NotDirectory(String desc) {
      super(desc);
    }

    NotDirectory() {}
  }

  /**
   * Exception for an invalid path.
   */
  public static class InvalidPath extends TreeException {
    InvalidPath(String desc) {
      super(desc);
    }

    InvalidPath() {}
  }

  /**
   * Exception for non-emtpy directory.
   */
  public static class DirectoryNotEmpty extends TreeException {
    DirectoryNotEmpty(String desc) {
      super(desc);
    }

    DirectoryNotEmpty() {}
  }

  /**
   * Exception for trying to delete root directory.
   */
  public static class DeleteRootDir extends TreeException {
    DeleteRootDir() {
      super("Can't delete root directory");
    }
  }

  /**
   * Exception for storing data on directory node.
   */
  public static class DirectoryNode extends TreeException {
    DirectoryNode(String desc) {
      super(desc);
    }

    DirectoryNode() {}
  }
}

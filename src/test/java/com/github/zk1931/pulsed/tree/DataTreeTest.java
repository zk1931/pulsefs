/**
 * Licensed to the zk1931 under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.zk1931.pulsed.tree;

import com.github.zk1931.pulsed.TestBase;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for DataTree implementation.
 */
public class DataTreeTest extends TestBase {
  private static final Logger LOG = LoggerFactory.getLogger(DataTreeTest.class);

  @Test
  public void testValidatePath1() throws Exception {
    DataTree tree = new DataTree();
    PathUtils.validatePath("/");
    PathUtils.validatePath("/foo");
    PathUtils.validatePath("/foo/bar");
    PathUtils.validatePath("/foo/bar/123/-sdaf-");
  }

  @Test(expected=DataTree.InvalidPath.class)
  public void testValidatePath2() throws Exception {
    DataTree tree = new DataTree();
    // Must start with '/'
    PathUtils.validatePath("foo");
  }

  @Test(expected=DataTree.InvalidPath.class)
  public void testValidatePath3() throws Exception {
    DataTree tree = new DataTree();
    // Can not end with '/'
    PathUtils.validatePath("/foo/");
  }

  @Test
  public void testRoot() throws Exception {
    DataTree tree = new DataTree();
    Assert.assertEquals(1, tree.size());
  }

  @Test
  public void testCreate() throws Exception {
    DataTree tree = new DataTree();
    Assert.assertEquals(1, tree.size());
    tree.createDir("/foo", false);
    Assert.assertEquals(2, tree.size());
    tree.createFile("/foo/bar1", null, false, false);
    tree.createFile("/foo/bar2", null, false, false);
    Assert.assertEquals(4, tree.size());
  }

  @Test(expected=DataTree.PathNotExist.class)
  public void testCreateUnderNonexistingPath() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo/bar", null, false, false);
  }

  @Test
  public void testRecursiveCreate() throws Exception {
    DataTree tree = new DataTree();
    // We'll create intermediate node /foo
    tree.createFile("/foo/bar", "helloworld".getBytes(), true, false);
    Assert.assertEquals(3, tree.size());
    Assert.assertTrue(Arrays.equals("helloworld".getBytes(),
                                    ((FileNode)tree.getNode("/foo/bar")).data));
  }

  @Test(expected=DataTree.NodeAlreadyExist.class)
  public void testDuplicateFile() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo", null, false, false);
    tree.createFile("/foo", null, false, false);
  }

  @Test(expected=DataTree.NotDirectory.class)
  public void testCreateUnderFile() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo", null, false, false);
    Assert.assertEquals(2, tree.size());
    tree.createFile("/foo/bar", null, false, false);
  }

  @Test
  public void testDeleteFile() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.createFile("/foo/bar1", null, false, false);
    tree.createFile("/foo/bar2", null, false, false);
    Assert.assertEquals(4, tree.size());
    tree.deleteNode("/foo/bar1", -1, false);
    Assert.assertEquals(3, tree.size());
    tree.deleteNode("/foo/bar2", -1, false);
    Assert.assertEquals(2, tree.size());
    tree.deleteNode("/foo", -1, false);
    Assert.assertEquals(1, tree.size());
  }

  @Test(expected=DataTree.DirectoryNotEmpty.class)
  public void testDeleteNonemptyDir() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.createFile("/foo/bar1", null, false, false);
    tree.createFile("/foo/bar2", null, false, false);
    tree.deleteNode("/foo", -1, false);
  }

  @Test
  public void testRecursiveDelete() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.createFile("/foo/bar1", null, false, false);
    tree.createFile("/foo/bar2", null, false, false);
    tree.deleteNode("/foo", -1, true);
    Assert.assertEquals(1, tree.size());
    Assert.assertEquals(4, tree.getNode("/").version);
  }

  @Test
  public void testDeleteWithMatchedVersion() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.createFile("/foo/bar1", null, false, false);
    tree.createFile("/foo/bar2", null, false, false);
    //Now the /foo/bar1 should have versio 0.
    tree.deleteNode("/foo/bar1", 0, false);
    //Now the /foo/bar2 should have versio 0.
    tree.deleteNode("/foo/bar2", 0, false);
    //Now the /foo should have versio 4.
    tree.deleteNode("/foo", 4, false);
  }

  @Test(expected=DataTree.VersionNotMatch.class)
  public void testDeleteWithUnmatchedVersion() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.createFile("/foo/bar1", null, false, false);
    tree.createFile("/foo/bar2", null, false, false);
    //Now the /foo/bar1 should have versio 0.
    tree.deleteNode("/foo/bar1", 0, false);
    //Now the /foo/bar2 should have versio 0.
    tree.deleteNode("/foo/bar2", 0, false);
    //Now the /foo should have versio 4.
    tree.deleteNode("/foo", 0, false);
  }

  @Test
  public void testDeleteWithMatchedVersionRecursive() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.createFile("/foo/bar1", null, false, false);
    tree.createFile("/foo/bar2", null, false, false);
    //Now the /foo should have versio 4.
    tree.deleteNode("/foo", 2, true);
    Assert.assertEquals(1, tree.size());
  }

  @Test
  public void testVersion() throws Exception {
    DataTree tree = new DataTree();
    // At begining root version is 0.
    Assert.assertEquals(0, tree.getNode("/").version);
    tree.createDir("/foo", false);
    Assert.assertEquals(1, tree.getNode("/").version);
    Assert.assertEquals(0, tree.getNode("/foo").version);
    tree.createFile("/foo/bar1", null, false, false);
    Assert.assertEquals(2, tree.getNode("/").version);
    Assert.assertEquals(1, tree.getNode("/foo").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar1").version);
    tree.createFile("/foo/bar2", null, false, false);
    Assert.assertEquals(3, tree.getNode("/").version);
    Assert.assertEquals(2, tree.getNode("/foo").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar1").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar2").version);
    tree.deleteNode("/foo/bar1", -1, false);
    Assert.assertEquals(4, tree.getNode("/").version);
    Assert.assertEquals(3, tree.getNode("/foo").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar2").version);
    // Now has 3 nodes.
    Assert.assertEquals(3, tree.size());
  }

  @Test(expected=DataTree.DeleteRootDir.class)
  public void testDeleteRoot() throws Exception {
    DataTree tree = new DataTree();
    tree.deleteNode("/", -1, true);
  }

  @Test
  public void testSetData() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.createFile("/foo/bar", null, false, false);
    Assert.assertEquals(2, tree.getNode("/").version);
    Assert.assertEquals(1, tree.getNode("/foo").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar").version);
    tree.setData("/foo/bar", "helloworld".getBytes(), -1);
    // All the nodes on the path will bump the version.
    Assert.assertEquals(3, tree.getNode("/").version);
    Assert.assertEquals(2, tree.getNode("/foo").version);
    Assert.assertEquals(1, tree.getNode("/foo/bar").version);
    Assert.assertTrue(Arrays.equals("helloworld".getBytes(),
                                    ((FileNode)tree.getNode("/foo/bar")).data));
  }

  @Test(expected=DataTree.PathNotExist.class)
  public void testSetDataOnUnexistPath() throws Exception {
    DataTree tree = new DataTree();
    tree.setData("/foo/bar", "helloworld".getBytes(), -1);
  }

  @Test
  public void testSetDataWithMatchedVersion() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo", null, false, false);
    Assert.assertEquals(0, tree.getNode("/foo").version);
    tree.setData("/foo", "helloworld1".getBytes(), 0);
    Assert.assertEquals(1, tree.getNode("/foo").version);
    tree.setData("/foo", "helloworld2".getBytes(), 1);
    Assert.assertEquals(2, tree.getNode("/foo").version);
    Assert.assertTrue(Arrays.equals("helloworld2".getBytes(),
                                    ((FileNode)tree.getNode("/foo")).data));
  }

  @Test(expected=DataTree.VersionNotMatch.class)
  public void testSetDataWithUnmatchedVersion() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo", null, false, false);
    Assert.assertEquals(0, tree.getNode("/foo").version);
    // Sets data with wrong version.
    tree.setData("/foo", "helloworld1".getBytes(), 1);
  }

  @Test(expected=DataTree.DirectoryNode.class)
  public void setDataOnDirectory() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.setData("/foo", "helloworld1".getBytes(), -1);
  }

  @Test
  public void testChildren() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", false);
    tree.createFile("/foo/bar1", null, false, false);
    tree.createFile("/foo/bar2", null, false, false);
    // /foo has two children bar1 and bar2
    Assert.assertEquals(2, ((DirNode)tree.getNode("/foo")).children.size());
    Assert.assertTrue(((DirNode)tree.getNode("/foo"))
                                    .children.containsKey("bar1"));
    Assert.assertTrue(((DirNode)tree.getNode("/foo"))
                                    .children.containsKey("bar2"));
    // Delete one of children.
    tree.deleteNode("/foo/bar1", -1, false);
    Assert.assertEquals(1, ((DirNode)tree.getNode("/foo")).children.size());
    Assert.assertFalse(((DirNode)tree.getNode("/foo"))
                                     .children.containsKey("bar1"));
    Assert.assertTrue(((DirNode)tree.getNode("/foo"))
                                    .children.containsKey("bar2"));
  }

  @Test
  public void testReturnChanges() throws Exception {
    DataTree tree = new DataTree();
    List<Node> changes = new LinkedList<Node>();
    Node createdNode = new FileNode("/foo/bar", 0, null);
    tree.root =
      tree.createNode(tree.root, createdNode, "foo/bar", true, false, changes);
    Assert.assertEquals(3, changes.size());
    // version of newly created node bar should be 1.
    Assert.assertEquals(0, changes.get(0).version);
    // version of newly created node foo should be 1.
    Assert.assertEquals(1, changes.get(1).version);
    // version of root node should be 1.
    Assert.assertEquals(1, changes.get(2).version);

    changes = new LinkedList<Node>();
    tree.root =
      (DirNode)tree.deleteNode(tree.root, "foo", -1, true, changes);
    // Two deleted nodes + one changed node(root node)
    Assert.assertEquals(3, changes.size());
    Assert.assertEquals(-1, changes.get(0).version);
    Assert.assertEquals(-1, changes.get(1).version);
    Assert.assertEquals(2, changes.get(2).version);
  }

  @Test
  public void testTransientFile() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo/bar/file", null, true, true);
    Assert.assertEquals(4, tree.size());
    tree.deleteNode("/foo/bar/file", -1, false);
    // Since /foo and /foo/bar are transient directories, they get deleted when
    // /foo/bar/file gets deleted.
    Assert.assertEquals(1, tree.size());
    Assert.assertFalse(tree.exist("/foo"));

    tree.createFile("/foo/bar/file1", null, true, true);
    tree.createFile("/foo/bar/file2", null, true, true);
    Assert.assertEquals(5, tree.size());
    tree.deleteNode("/foo/bar/file1", -1, false);
    // Although /foo and /foo/bar are transient directories, since
    // /foo/bar/file2 still exists, so they will not be deleted.
    Assert.assertEquals(4, tree.size());
    Assert.assertTrue(tree.exist("/foo"));
    Assert.assertTrue(tree.exist("/foo/bar"));
    // Now delete the last file file2.
    tree.deleteNode("/foo/bar/file2", -1, false);
    // After deleting last file, transient directories get deleted.
    Assert.assertEquals(1, tree.size());
    Assert.assertFalse(tree.exist("/foo"));
    Assert.assertFalse(tree.exist("/foo/bar"));
  }

  @Test
  public void testSessionFile() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/session1", false);
    tree.createSessionFile("/session1/file1", null, 1, false, false);
    tree.createSessionFile("/session1/file2", null, 1, false, false);
    tree.createDir("/session2", false);
    tree.createSessionFile("/session2/file1", null, 2, false, false);
    tree.createSessionFile("/session2/file2", null, 2, false, false);
    tree.createSessionFile("/session2/file3", null, 2, false, false);
    // Now we have 8 nodes in total.
    Assert.assertEquals(8, tree.size());
    // Deletes one file of session2.
    tree.deleteNode("/session2/file2", -1, false);
    // Now we have 7 nodes in total.
    Assert.assertEquals(7, tree.size());
    // Deletes all the files of session 1.
    tree.deleteSession(1);
    // Now we have 5 nodes in total.
    Assert.assertEquals(5, tree.size());
    Assert.assertFalse(tree.exist("/session1/file1"));
    Assert.assertFalse(tree.exist("/session1/file2"));
    // Deletes all the files of session 2.
    tree.deleteSession(2);
    // Now we have 3 nodes in total.
    Assert.assertEquals(3, tree.size());
    Assert.assertFalse(tree.exist("/session2/file1"));
    Assert.assertFalse(tree.exist("/session2/file2"));

    // Deletes all the files of session 3, which doesn't exist.
    tree.deleteSession(3);
    // Now we have 3 nodes in total.
    Assert.assertEquals(3, tree.size());
    Assert.assertFalse(tree.exist("/session2/file1"));

    // Creates files with session 1 again.
    tree.createSessionFile("/session1/file1", null, 1, false, false);
    tree.createSessionFile("/session1/file2", null, 1, false, false);
    Assert.assertEquals(5, tree.size());
    Assert.assertTrue(tree.exist("/session1/file1"));
    Assert.assertTrue(tree.exist("/session1/file2"));
  }
}

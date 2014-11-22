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

package com.github.zk1931.pulsed;

import java.util.Arrays;
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
    tree.validatePath("/");
    tree.validatePath("/foo");
    tree.validatePath("/foo/bar");
    tree.validatePath("/foo/bar/123/-sdaf-");
  }

  @Test(expected=DataTree.InvalidPath.class)
  public void testValidatePath2() throws Exception {
    DataTree tree = new DataTree();
    // Must start with '/'
    tree.validatePath("foo");
  }

  @Test(expected=DataTree.InvalidPath.class)
  public void testValidatePath3() throws Exception {
    DataTree tree = new DataTree();
    // Can not end with '/'
    tree.validatePath("/foo/");
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
    tree.createDir("/foo", 0, false);
    Assert.assertEquals(2, tree.size());
    tree.createFile("/foo/bar1", null, 0, false);
    tree.createFile("/foo/bar2", null, 0, false);
    Assert.assertEquals(4, tree.size());
  }

  @Test(expected=DataTree.PathNotExist.class)
  public void testCreateUnderNonexistingPath() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo/bar", null, 0, false);
  }

  @Test
  public void testRecursiveCreate() throws Exception {
    DataTree tree = new DataTree();
    // We'll create intermediate node /foo
    tree.createFile("/foo/bar", "helloworld".getBytes(), 0, true);
    Assert.assertEquals(3, tree.size());
    Assert.assertTrue(Arrays.equals("helloworld".getBytes(),
                                    ((FileNode)tree.getNode("/foo/bar")).data));
  }

  @Test(expected=DataTree.NodeAlreadyExist.class)
  public void testDuplicateFile() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo", null, 0, false);
    tree.createFile("/foo", null, 0, false);
  }

  @Test(expected=DataTree.NotDirectory.class)
  public void testCreateUnderFile() throws Exception {
    DataTree tree = new DataTree();
    tree.createFile("/foo", null, 0, false);
    Assert.assertEquals(2, tree.size());
    tree.createFile("/foo/bar", null, 0, false);
  }

  @Test
  public void testDeleteFile() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", 0, false);
    tree.createFile("/foo/bar1", null, 0, false);
    tree.createFile("/foo/bar2", null, 0, false);
    Assert.assertEquals(4, tree.size());
    tree.deleteNode("/foo/bar1", false);
    Assert.assertEquals(3, tree.size());
    tree.deleteNode("/foo/bar2", false);
    Assert.assertEquals(2, tree.size());
    tree.deleteNode("/foo", false);
    Assert.assertEquals(1, tree.size());
  }

  @Test(expected=DataTree.DirectoryNotEmpty.class)
  public void testDeleteNonemptyDir() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", 0, false);
    tree.createFile("/foo/bar1", null, 0, false);
    tree.createFile("/foo/bar2", null, 0, false);
    tree.deleteNode("/foo", false);
  }

  @Test
  public void testRecursiveDelete() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", 0, false);
    tree.createFile("/foo/bar1", null, 0, false);
    tree.createFile("/foo/bar2", null, 0, false);
    tree.deleteNode("/foo", true);
    Assert.assertEquals(1, tree.size());
    Assert.assertEquals(4, tree.getNode("/").version);
  }

  @Test
  public void testVersion() throws Exception {
    DataTree tree = new DataTree();
    // At begining root version is 0.
    Assert.assertEquals(0, tree.getNode("/").version);
    tree.createDir("/foo", 0, false);
    Assert.assertEquals(1, tree.getNode("/").version);
    Assert.assertEquals(0, tree.getNode("/foo").version);
    tree.createFile("/foo/bar1", null, 0, false);
    Assert.assertEquals(2, tree.getNode("/").version);
    Assert.assertEquals(1, tree.getNode("/foo").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar1").version);
    tree.createFile("/foo/bar2", null, 0, false);
    Assert.assertEquals(3, tree.getNode("/").version);
    Assert.assertEquals(2, tree.getNode("/foo").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar1").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar2").version);
    tree.deleteNode("/foo/bar1", false);
    Assert.assertEquals(4, tree.getNode("/").version);
    Assert.assertEquals(3, tree.getNode("/foo").version);
    Assert.assertEquals(0, tree.getNode("/foo/bar2").version);
    // Now has 3 nodes.
    Assert.assertEquals(3, tree.size());
  }

  @Test(expected=DataTree.DeleteRootDir.class)
  public void testDeleteRoot() throws Exception {
    DataTree tree = new DataTree();
    tree.deleteNode("/", true);
  }

  @Test
  public void testSetData() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", 0, false);
    tree.createFile("/foo/bar", null, 0, false);
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
    tree.createFile("/foo", null, 0, false);
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
    tree.createFile("/foo", null, 0, false);
    Assert.assertEquals(0, tree.getNode("/foo").version);
    // Sets data with wrong version.
    tree.setData("/foo", "helloworld1".getBytes(), 1);
  }

  @Test(expected=DataTree.DirectoryNode.class)
  public void setDataOnDirectory() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", 0, false);
    tree.setData("/foo", "helloworld1".getBytes(), -1);
  }

  @Test
  public void testChildren() throws Exception {
    DataTree tree = new DataTree();
    tree.createDir("/foo", 0, false);
    tree.createFile("/foo/bar1", null, 0, false);
    tree.createFile("/foo/bar2", null, 0, false);
    // /foo has two children bar1 and bar2
    Assert.assertEquals(2, ((DirNode)tree.getNode("/foo")).children.size());
    Assert.assertTrue(((DirNode)tree.getNode("/foo"))
                                    .children.containsKey("bar1"));
    Assert.assertTrue(((DirNode)tree.getNode("/foo"))
                                    .children.containsKey("bar2"));
    // Delete one of children.
    tree.deleteNode("/foo/bar1", false);
    Assert.assertEquals(1, ((DirNode)tree.getNode("/foo")).children.size());
    Assert.assertFalse(((DirNode)tree.getNode("/foo"))
                                     .children.containsKey("bar1"));
    Assert.assertTrue(((DirNode)tree.getNode("/foo"))
                                    .children.containsKey("bar2"));
  }
}

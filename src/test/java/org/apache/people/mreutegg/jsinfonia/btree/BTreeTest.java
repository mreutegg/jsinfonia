/*
 * Copyright 2013 Marcel Reutegger
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.people.mreutegg.jsinfonia.btree;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.AbstractTransactionTest;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerFactory;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BTreeTest extends AbstractTransactionTest {

  private ItemReference itemManagerRef;
  private ItemReference btreeMetadataRef;

  @Override
  protected MemoryNodeDirectory<? extends MemoryNode> createDirectory() {
    return createDirectory(1, 1024, 1024, 128);
  }

  @BeforeEach
  void setUpRefs() {
    TransactionManager txManager = createTransactionContext();
    itemManagerRef = txManager.execute(txContext -> ItemManagerImpl.initialize(txContext, 0, 1024));
    btreeMetadataRef =
        txManager.execute(
            txContext -> {
              ItemManager itemMgr = new ItemManagerImpl(txContext, itemManagerRef);
              return itemMgr.alloc();
            });
  }

  @Test
  void insertAndLookup() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef);
    btree.initialize();

    btree.insert("key1", "value1".getBytes());
    btree.insert("key2", "value2".getBytes());

    assertArrayEquals("value1".getBytes(), btree.lookup("key1"));
    assertArrayEquals("value2".getBytes(), btree.lookup("key2"));
    assertNull(btree.lookup("key3"));
  }

  @Test
  void split() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    // Small maxKeys to trigger split early
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    btree.initialize();

    for (int i = 0; i < 20; i++) {
      btree.insert("key" + i, ("value" + i).getBytes());
    }

    for (int i = 0; i < 20; i++) {
      assertArrayEquals(
          ("value" + i).getBytes(),
          btree.lookup("key" + i),
          "Value for key" + i + " should be correct");
    }
  }

  @Test
  void update() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef);
    btree.initialize();

    btree.insert("key1", "value1".getBytes());
    assertArrayEquals("value1".getBytes(), btree.lookup("key1"));

    assertTrue(btree.update("key1", "value1-updated".getBytes()));
    assertArrayEquals("value1-updated".getBytes(), btree.lookup("key1"));
  }

  @Test
  void delete() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef);
    btree.initialize();

    btree.insert("key1", "value1".getBytes());
    assertNotNull(btree.lookup("key1"));

    btree.delete("key1");
    assertNull(btree.lookup("key1"));
  }

  @Test
  void deletePredecessorReplace() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    btree.initialize();

    btree.insert("key2", "value2".getBytes());
    btree.insert("key4", "value4".getBytes());
    btree.insert("key6", "value6".getBytes());
    btree.insert("key8", "value8".getBytes()); // triggers split: root key = "key6"

    // Delete "key6" which is in the root internal node
    // preceding child y has {"key2", "key4"} (size 2 >= minKeys + 1)
    // successor z has {"key6", "key8"}
    // This triggers Case 2a (predecessor replace)
    btree.delete("key6");

    assertNull(btree.lookup("key6"));
    assertArrayEquals("value2".getBytes(), btree.lookup("key2"));
    assertArrayEquals("value4".getBytes(), btree.lookup("key4"));
    assertArrayEquals("value8".getBytes(), btree.lookup("key8"));
  }

  @Test
  void deleteBorrowFromLeftSibling() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    btree.initialize();

    btree.insert("key2", "value2".getBytes());
    btree.insert("key4", "value4".getBytes());
    btree.insert("key6", "value6".getBytes());
    btree.insert("key8", "value8".getBytes()); // split

    // Right leaf has {"key6", "key8"}
    // Delete "key8" so right leaf has {"key6"} (size 1 == minKeys)
    btree.delete("key8");

    // Now delete non-existent "key7" (must descend to right leaf)
    // Left leaf has {"key2", "key4"} (size 2 >= minKeys + 1)
    // This triggers Case 3a (borrow from left sibling)
    assertFalse(btree.delete("key7"));

    assertArrayEquals("value2".getBytes(), btree.lookup("key2"));
    assertArrayEquals("value4".getBytes(), btree.lookup("key4"));
    assertArrayEquals("value6".getBytes(), btree.lookup("key6"));
    assertNull(btree.lookup("key8"));
  }

  @Test
  void deleteBorrowFromRightSibling() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    btree.initialize();

    btree.insert("key2", "value2".getBytes());
    btree.insert("key4", "value4".getBytes());
    btree.insert("key6", "value6".getBytes());
    btree.insert("key8", "value8".getBytes()); // split

    // Left leaf has {"key2", "key4"}
    // Delete "key2" so left leaf has {"key4"} (size 1 == minKeys)
    btree.delete("key2");

    // Now delete non-existent "key3" (must descend to left leaf)
    // Right leaf has {"key6", "key8"} (size 2 >= minKeys + 1)
    // This triggers Case 3a (borrow from right sibling)
    assertFalse(btree.delete("key3"));

    assertNull(btree.lookup("key2"));
    assertArrayEquals("value4".getBytes(), btree.lookup("key4"));
    assertArrayEquals("value6".getBytes(), btree.lookup("key6"));
    assertArrayEquals("value8".getBytes(), btree.lookup("key8"));
  }

  @Test
  void deleteMergeLeafNodesAndHeightShrink() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    btree.initialize();

    btree.insert("key2", "value2".getBytes());
    btree.insert("key4", "value4".getBytes());
    btree.insert("key6", "value6".getBytes());
    btree.insert("key8", "value8".getBytes()); // split

    // Delete "key2" (left has {"key4"})
    btree.delete("key2");
    // Delete "key8" (right has {"key6"})
    btree.delete("key8");

    // Delete "key6" (in root, preceding has size 1, succeeding has size 1)
    // This triggers Case 2c (merge leaf nodes) and root height shrinking
    btree.delete("key6");

    assertNull(btree.lookup("key2"));
    assertNull(btree.lookup("key6"));
    assertNull(btree.lookup("key8"));
    assertArrayEquals("value4".getBytes(), btree.lookup("key4"));
  }

  @Test
  void deleteRandomStressTest() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    btree.initialize();

    // Insert 100 entries
    for (int i = 0; i < 100; i++) {
      btree.insert(String.format("k%03d", i), ("val" + i).getBytes());
    }

    // Verify all present
    for (int i = 0; i < 100; i++) {
      assertArrayEquals(("val" + i).getBytes(), btree.lookup(String.format("k%03d", i)));
    }

    // Delete all even keys
    for (int i = 0; i < 100; i += 2) {
      btree.delete(String.format("k%03d", i));
    }

    // Verify even keys deleted, odd keys still present
    for (int i = 0; i < 100; i++) {
      String key = String.format("k%03d", i);
      if (i % 2 == 0) {
        assertNull(btree.lookup(key));
      } else {
        assertArrayEquals(("val" + i).getBytes(), btree.lookup(key));
      }
    }

    // Delete all odd keys
    for (int i = 1; i < 100; i += 2) {
      btree.delete(String.format("k%03d", i));
    }

    // Verify everything is null
    for (int i = 0; i < 100; i++) {
      assertNull(btree.lookup(String.format("k%03d", i)));
    }
  }

  @Test
  void leafSiblingLinks() throws IOException {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    btree.initialize();

    // Insert 20 keys to trigger multiple splits and create a B+ Tree structure with several leaf nodes
    for (int i = 0; i < 20; i++) {
      btree.insert(String.format("key%02d", i), ("value" + i).getBytes());
    }

    // Now, run a transaction to traverse the leaves via next links
    txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, btreeMetadataRef);
          ItemReference currentRef = metadata.getRootNodeRef();
          BTreeNode<String, byte[]> node = BTreeNode.load(txContext, currentRef, StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
          
          // Traverse down to the first leaf node
          while (node instanceof InternalNode<String, byte[]> internal) {
            currentRef = internal.getChild(0);
            node = BTreeNode.load(txContext, currentRef, StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
          }
          
          LeafNode<String, byte[]> firstLeaf = (LeafNode<String, byte[]>) node;
          
          // 1. Traverse forward using getNext()
          java.util.List<String> forwardKeys = new java.util.ArrayList<>();
          LeafNode<String, byte[]> curr = firstLeaf;
          ItemReference lastRef = null;
          while (curr != null) {
            forwardKeys.addAll(curr.keys);
            lastRef = curr.getReference();
            if (curr.getNext() != null) {
              curr = (LeafNode<String, byte[]>) BTreeNode.load(txContext, curr.getNext(), StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
            } else {
              curr = null;
            }
          }
          
          // Verify we saw all 20 keys in order
          assertEquals(20, forwardKeys.size());
          for (int i = 0; i < 20; i++) {
            assertEquals(String.format("key%02d", i), forwardKeys.get(i));
          }
          
          // 2. Traverse backward using getPrev() starting from lastRef
          java.util.List<String> backwardKeys = new java.util.ArrayList<>();
          curr = (LeafNode<String, byte[]>) BTreeNode.load(txContext, lastRef, StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
          while (curr != null) {
            // prepend keys in reverse order of leaf keys
            for (int i = curr.getKeyCount() - 1; i >= 0; i--) {
              backwardKeys.add(0, curr.getKey(i));
            }
            if (curr.getPrev() != null) {
              curr = (LeafNode<String, byte[]>) BTreeNode.load(txContext, curr.getPrev(), StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
            } else {
              curr = null;
            }
          }
          
          // Verify backward keys
          assertEquals(20, backwardKeys.size());
          for (int i = 0; i < 20; i++) {
            assertEquals(String.format("key%02d", i), backwardKeys.get(i));
          }
          
          return null;
        });
        
    // Delete some keys to trigger merges and verify links are correctly maintained
    for (int i = 0; i < 10; i++) {
      btree.delete(String.format("key%02d", i));
    }
    
    // Traverse again and verify 10 remaining keys
    txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, btreeMetadataRef);
          ItemReference currentRef = metadata.getRootNodeRef();
          BTreeNode<String, byte[]> node = BTreeNode.load(txContext, currentRef, StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
          
          // Traverse down to the first leaf node
          while (node instanceof InternalNode<String, byte[]> internal) {
            currentRef = internal.getChild(0);
            node = BTreeNode.load(txContext, currentRef, StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
          }
          
          LeafNode<String, byte[]> firstLeaf = (LeafNode<String, byte[]>) node;
          
          // Traverse forward using getNext()
          java.util.List<String> forwardKeys = new java.util.ArrayList<>();
          LeafNode<String, byte[]> curr = firstLeaf;
          ItemReference lastRef = null;
          while (curr != null) {
            forwardKeys.addAll(curr.keys);
            lastRef = curr.getReference();
            if (curr.getNext() != null) {
              curr = (LeafNode<String, byte[]>) BTreeNode.load(txContext, curr.getNext(), StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
            } else {
              curr = null;
            }
          }
          
          // Verify we saw remaining 10 keys in order
          assertEquals(10, forwardKeys.size());
          for (int i = 0; i < 10; i++) {
            assertEquals(String.format("key%02d", i + 10), forwardKeys.get(i));
          }
          
          // Traverse backward using getPrev()
          java.util.List<String> backwardKeys = new java.util.ArrayList<>();
          curr = (LeafNode<String, byte[]>) BTreeNode.load(txContext, lastRef, StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
          while (curr != null) {
            for (int i = curr.getKeyCount() - 1; i >= 0; i--) {
              backwardKeys.add(0, curr.getKey(i));
            }
            if (curr.getPrev() != null) {
              curr = (LeafNode<String, byte[]>) BTreeNode.load(txContext, curr.getPrev(), StringSerializer.INSTANCE, ByteArraySerializer.INSTANCE);
            } else {
              curr = null;
            }
          }
          
          // Verify backward keys
          assertEquals(10, backwardKeys.size());
          for (int i = 0; i < 10; i++) {
            assertEquals(String.format("key%02d", i + 10), backwardKeys.get(i));
          }
          
          return null;
        });
  }

  @Test
  void closestMatchLookups() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    
    // 1. Test on empty tree
    btree.initialize();
    assertNull(btree.floorEntry("key"));
    assertNull(btree.floorKey("key"));
    assertNull(btree.ceilingEntry("key"));
    assertNull(btree.ceilingKey("key"));
    assertNull(btree.lowerEntry("key"));
    assertNull(btree.lowerKey("key"));
    assertNull(btree.higherEntry("key"));
    assertNull(btree.higherKey("key"));

    // Populate the B-Tree with key10, key20, key30
    btree.insert("key10", "value10".getBytes());
    btree.insert("key20", "value20".getBytes());
    btree.insert("key30", "value30".getBytes());

    // 2. Test floor
    // exact match
    assertEquals("key20", btree.floorKey("key20"));
    assertArrayEquals("value20".getBytes(), btree.floorEntry("key20").getValue());
    // non-exact match (between two keys)
    assertEquals("key20", btree.floorKey("key25"));
    assertArrayEquals("value20".getBytes(), btree.floorEntry("key25").getValue());
    // smaller than all keys
    assertNull(btree.floorKey("key05"));
    assertNull(btree.floorEntry("key05"));
    // larger than all keys
    assertEquals("key30", btree.floorKey("key35"));
    assertArrayEquals("value30".getBytes(), btree.floorEntry("key35").getValue());

    // 3. Test ceiling
    // exact match
    assertEquals("key20", btree.ceilingKey("key20"));
    assertArrayEquals("value20".getBytes(), btree.ceilingEntry("key20").getValue());
    // non-exact match (between two keys)
    assertEquals("key20", btree.ceilingKey("key15"));
    assertArrayEquals("value20".getBytes(), btree.ceilingEntry("key15").getValue());
    // smaller than all keys
    assertEquals("key10", btree.ceilingKey("key05"));
    assertArrayEquals("value10".getBytes(), btree.ceilingEntry("key05").getValue());
    // larger than all keys
    assertNull(btree.ceilingKey("key35"));
    assertNull(btree.ceilingEntry("key35"));

    // 4. Test lower (strictly less than)
    // exact match
    assertEquals("key10", btree.lowerKey("key20"));
    assertArrayEquals("value10".getBytes(), btree.lowerEntry("key20").getValue());
    // non-exact match
    assertEquals("key20", btree.lowerKey("key25"));
    assertArrayEquals("value20".getBytes(), btree.lowerEntry("key25").getValue());
    // smaller than or equal to first key
    assertNull(btree.lowerKey("key10"));
    assertNull(btree.lowerEntry("key10"));
    assertNull(btree.lowerKey("key05"));

    // 5. Test higher (strictly greater than)
    // exact match
    assertEquals("key30", btree.higherKey("key20"));
    assertArrayEquals("value30".getBytes(), btree.higherEntry("key20").getValue());
    // non-exact match
    assertEquals("key20", btree.higherKey("key15"));
    assertArrayEquals("value20".getBytes(), btree.higherEntry("key15").getValue());
    // larger than or equal to last key
    assertNull(btree.higherKey("key30"));
    assertNull(btree.higherEntry("key30"));
    assertNull(btree.higherKey("key35"));
  }

  @Test
  void rangeBoundedIterators() {
    TransactionManager txManager = createTransactionContext();
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    
    // 1. Test empty tree iterator
    btree.initialize();
    java.util.Iterator<String> it = btree.keyIterator(null, true, null, true, false);
    assertFalse(it.hasNext());

    // Populate the B-Tree with key00, key01, ..., key19
    for (int i = 0; i < 20; i++) {
      btree.insert(String.format("key%02d", i), ("value" + i).getBytes());
    }

    // 2. Full range forward iterator
    java.util.List<String> list = new java.util.ArrayList<>();
    btree.keyIterator(null, true, null, true, false).forEachRemaining(list::add);
    assertEquals(20, list.size());
    assertEquals("key00", list.get(0));
    assertEquals("key19", list.get(19));

    // 3. Full range backward (descending) iterator
    list.clear();
    btree.keyIterator(null, true, null, true, true).forEachRemaining(list::add);
    assertEquals(20, list.size());
    assertEquals("key19", list.get(0));
    assertEquals("key00", list.get(19));

    // 4. Bounded range forward: key05 (inclusive) to key15 (exclusive)
    list.clear();
    btree.keyIterator("key05", true, "key15", false, false).forEachRemaining(list::add);
    assertEquals(10, list.size());
    assertEquals("key05", list.get(0));
    assertEquals("key14", list.get(9));

    // 5. Bounded range forward: key05 (exclusive) to key15 (inclusive)
    list.clear();
    btree.keyIterator("key05", false, "key15", true, false).forEachRemaining(list::add);
    assertEquals(10, list.size());
    assertEquals("key06", list.get(0));
    assertEquals("key15", list.get(9));

    // 6. Bounded range backward (descending): key15 (inclusive) to key05 (exclusive)
    list.clear();
    btree.keyIterator("key15", true, "key05", false, true).forEachRemaining(list::add);
    assertEquals(10, list.size());
    assertEquals("key15", list.get(0));
    assertEquals("key06", list.get(9));

    // 7. Bounded range backward (descending): key15 (exclusive) to key05 (inclusive)
    list.clear();
    btree.keyIterator("key15", false, "key05", true, true).forEachRemaining(list::add);
    assertEquals(10, list.size());
    assertEquals("key14", list.get(0));
    assertEquals("key05", list.get(9));
    
    // 8. Bounded range when bounds do not exist: key05a (inclusive) to key15a (inclusive)
    list.clear();
    btree.keyIterator("key05a", true, "key15a", true, false).forEachRemaining(list::add);
    // Elements should be key06 to key15
    assertEquals(10, list.size());
    assertEquals("key06", list.get(0));
    assertEquals("key15", list.get(9));
  }
}

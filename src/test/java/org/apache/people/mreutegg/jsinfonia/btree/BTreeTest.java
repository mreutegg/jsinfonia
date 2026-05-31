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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef);
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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef, 4);
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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef);
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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef);
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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef, 4);
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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef, 4);
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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef, 4);
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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef, 4);
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
    BTree btree = new BTree(txManager, factory, btreeMetadataRef, 4);
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
}

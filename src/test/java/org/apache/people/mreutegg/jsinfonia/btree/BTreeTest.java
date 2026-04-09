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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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

    btree.update("key1", "value1-updated".getBytes());
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
}

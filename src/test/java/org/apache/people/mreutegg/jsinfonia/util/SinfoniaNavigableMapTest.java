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
package org.apache.people.mreutegg.jsinfonia.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.AbstractTransactionTest;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.btree.BTree;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SinfoniaNavigableMapTest extends AbstractTransactionTest {

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

  private BTree<String, byte[]> createBTree(TransactionManager txManager) {
    ItemManagerFactory factory = txContext -> new ItemManagerImpl(txContext, itemManagerRef);
    BTree<String, byte[]> btree = new BTree<>(txManager, factory, btreeMetadataRef, 4);
    return btree;
  }

  @Test
  void testBasicMapOperations() {
    TransactionManager txManager = createTransactionContext();
    BTree<String, byte[]> btree = createBTree(txManager);
    btree.initialize();

    NavigableMap<String, byte[]> map = new SinfoniaNavigableMap<>(btree);
    assertTrue(map.isEmpty());
    assertEquals(0, map.size());

    assertNull(map.put("k2", "v2".getBytes()));
    assertNull(map.put("k1", "v1".getBytes()));
    assertNull(map.put("k3", "v3".getBytes()));

    assertFalse(map.isEmpty());
    assertEquals(3, map.size());

    assertArrayEquals("v1".getBytes(), map.get("k1"));
    assertArrayEquals("v2".getBytes(), map.get("k2"));
    assertArrayEquals("v3".getBytes(), map.get("k3"));

    // Overwrite
    assertArrayEquals("v2".getBytes(), map.put("k2", "v2-new".getBytes()));
    assertArrayEquals("v2-new".getBytes(), map.get("k2"));
    assertEquals(3, map.size());

    // Remove
    assertArrayEquals("v2-new".getBytes(), map.remove("k2"));
    assertNull(map.get("k2"));
    assertEquals(2, map.size());
  }

  @Test
  void testSubMapViews() {
    TransactionManager txManager = createTransactionContext();
    BTree<String, byte[]> btree = createBTree(txManager);
    btree.initialize();

    NavigableMap<String, byte[]> map = new SinfoniaNavigableMap<>(btree);
    for (int i = 0; i < 10; i++) {
      map.put("key" + i, ("val" + i).getBytes());
    }

    // subMap from key2 (inclusive) to key7 (exclusive)
    NavigableMap<String, byte[]> sub = map.subMap("key2", true, "key7", false);
    assertEquals(5, sub.size());
    assertTrue(sub.containsKey("key2"));
    assertTrue(sub.containsKey("key6"));
    assertFalse(sub.containsKey("key7"));

    // headMap to key4 (inclusive)
    NavigableMap<String, byte[]> head = map.headMap("key4", true);
    assertEquals(5, head.size()); // key0, key1, key2, key3, key4
    assertTrue(head.containsKey("key0"));
    assertTrue(head.containsKey("key4"));
    assertFalse(head.containsKey("key5"));

    // tailMap from key6 (exclusive)
    NavigableMap<String, byte[]> tail = map.tailMap("key6", false);
    assertEquals(3, tail.size()); // key7, key8, key9
    assertFalse(tail.containsKey("key6"));
    assertTrue(tail.containsKey("key7"));
  }

  @Test
  void testDescendingMap() {
    TransactionManager txManager = createTransactionContext();
    BTree<String, byte[]> btree = createBTree(txManager);
    btree.initialize();

    NavigableMap<String, byte[]> map = new SinfoniaNavigableMap<>(btree);
    map.put("k1", "v1".getBytes());
    map.put("k2", "v2".getBytes());
    map.put("k3", "v3".getBytes());

    NavigableMap<String, byte[]> desc = map.descendingMap();
    assertEquals(3, desc.size());

    // First and last
    assertEquals("k3", desc.firstKey());
    assertEquals("k1", desc.lastKey());

    // Iteration order
    Iterator<String> it = desc.keySet().iterator();
    assertEquals("k3", it.next());
    assertEquals("k2", it.next());
    assertEquals("k1", it.next());
    assertFalse(it.hasNext());

    // Closest match lookups on descending
    assertEquals("k2", desc.floorKey("k2"));
    assertEquals("k3", desc.floorKey("k25"));
    assertEquals("k2", desc.floorKey("k15"));
  }
}

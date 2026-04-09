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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.DataItemCache;
import org.apache.people.mreutegg.jsinfonia.data.Transaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FixedBitSetTest {

  private static final Logger log = LoggerFactory.getLogger(FixedBitSetTest.class);

  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private ApplicationNode appNode;

  @Test
  void bitSet() {
    for (int i = 0; i < 3; i++) {
      SimpleMemoryNodeDirectory<InMemoryMemoryNode> directory = new SimpleMemoryNodeDirectory<>();
      directory.addMemoryNode(new InMemoryMemoryNode(0, 1024, 1024));
      appNode = new SimpleApplicationNode(directory, EXECUTOR);
      doTest((int) (Math.random() * 83) + 1);
    }
  }

  private void doTest(int numDataItems) {
    final ItemReference headerRef = new ItemReference(0, 0);
    final List<ItemReference> dataItemRefs = new ArrayList<>();
    for (int i = 1; i <= numDataItems; i++) {
      dataItemRefs.add(new ItemReference(0, i));
    }
    TransactionManager txMgr = new TransactionManager(appNode, new DataItemCache(1024));
    FixedBitSet bitSet =
        txMgr.execute(txContext -> new FixedBitSet(txContext, headerRef, dataItemRefs));
    log.info(
        "created FixedBitSet with " + numDataItems + " data items and length: " + bitSet.length());

    for (int i = 0; i < bitSet.length(); i++) {
      // initially all bits are false
      assertFalse(bitSet.get(i), "bit " + i + " has wrong value");
    }

    for (int i = 0; i < 1000; i++) {
      final int index = (int) (Math.random() * bitSet.length());
      final boolean bit =
          txMgr.execute(txContext -> new FixedBitSet(txContext, headerRef).get(index));
      // now flip the bit
      txMgr.execute(
          (Transaction<Void>)
              txContext -> {
                FixedBitSet bitSet1 = new FixedBitSet(txContext, headerRef);
                if (bit) {
                  bitSet1.clear(index);
                } else {
                  bitSet1.set(index);
                }
                return null;
              });
      // assert bit is flipped
      boolean flipped =
          txMgr.execute(txContext -> new FixedBitSet(txContext, headerRef).get(index));
      assertEquals(!bit, flipped, "Bit at " + index + " not flipped.");
    }

    // collect all bits set true
    List<Integer> bitsSetTrue =
        txMgr.execute(
            txContext -> {
              FixedBitSet bitSet2 = new FixedBitSet(txContext, headerRef);
              List<Integer> list = new ArrayList<>();
              for (int i = 0; i < bitSet2.length(); i++) {
                if (bitSet2.get(i)) {
                  list.add(i);
                }
              }
              return list;
            });
    final int[] index = new int[] {0};
    for (final Integer i : bitsSetTrue) {
      index[0] =
          txMgr.execute(
              txContext -> {
                FixedBitSet bitSet3 = new FixedBitSet(txContext, headerRef);
                return bitSet3.nextSetBit(index[0]);
              });
      assertEquals((int) i, index[0]);
      index[0] = index[0] + 1;
    }
  }

  @Test
  void nextClearBit() {
    final int numDataItems = 4;
    SimpleMemoryNodeDirectory<InMemoryMemoryNode> directory = new SimpleMemoryNodeDirectory<>();
    directory.addMemoryNode(new InMemoryMemoryNode(0, 1024, 1024));
    appNode = new SimpleApplicationNode(directory, EXECUTOR);
    final ItemReference headerRef = new ItemReference(0, 0);
    final List<ItemReference> dataItemRefs = new ArrayList<>();
    for (int i = 1; i <= numDataItems; i++) {
      dataItemRefs.add(new ItemReference(0, i));
    }
    TransactionManager txMgr = new TransactionManager(appNode, new DataItemCache(1024));
    FixedBitSet bitSet =
        txMgr.execute(txContext -> new FixedBitSet(txContext, headerRef, dataItemRefs));
    log.info(
        "created FixedBitSet with " + numDataItems + " data items and length: {}", bitSet.length());

    for (int i = 0; i < bitSet.length() / 2; i++) {
      final int index = (int) (Math.random() * bitSet.length());
      // set random bit
      txMgr.execute(
          (Transaction<Void>)
              txContext -> {
                FixedBitSet bitSet1 = new FixedBitSet(txContext, headerRef);
                bitSet1.set(index);
                return null;
              });
    }

    // collect all bits set false
    List<Integer> bitsSetFalse =
        txMgr.execute(
            txContext -> {
              FixedBitSet bitSet2 = new FixedBitSet(txContext, headerRef);
              List<Integer> list = new ArrayList<>();
              for (int i = 0; i < bitSet2.length(); i++) {
                if (!bitSet2.get(i)) {
                  list.add(i);
                }
              }
              return list;
            });
    final int[] index = new int[] {0};
    for (final Integer i : bitsSetFalse) {
      index[0] =
          txMgr.execute(
              txContext -> {
                FixedBitSet bitSet3 = new FixedBitSet(txContext, headerRef);
                return bitSet3.nextClearBit(index[0]);
              });
      assertEquals((int) i, index[0]);
      index[0] = index[0] + 1;
    }
  }
}

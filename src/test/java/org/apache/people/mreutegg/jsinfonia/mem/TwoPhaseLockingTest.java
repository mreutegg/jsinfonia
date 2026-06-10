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
package org.apache.people.mreutegg.jsinfonia.mem;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Response;
import org.apache.people.mreutegg.jsinfonia.Result;
import org.apache.people.mreutegg.jsinfonia.SimpleApplicationNode;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.junit.jupiter.api.Test;

class TwoPhaseLockingTest {

  @Test
  void testFracturedRead() throws Exception {
    final CountDownLatch t1ReadNode0Latch = new CountDownLatch(1);
    final CountDownLatch t2CommitLatch = new CountDownLatch(1);
    final CountDownLatch t1AllowedToReadNode1Latch = new CountDownLatch(1);

    SimpleMemoryNodeDirectory<MemoryNode> directory = new SimpleMemoryNodeDirectory<>();
    CoordinatedMemoryNode node0 = new CoordinatedMemoryNode(0, 1024, 64,
        t1ReadNode0Latch, t2CommitLatch, t1AllowedToReadNode1Latch);
    CoordinatedMemoryNode node1 = new CoordinatedMemoryNode(1, 1024, 64,
        t1ReadNode0Latch, t2CommitLatch, t1AllowedToReadNode1Latch);
    directory.addMemoryNode(node0);
    directory.addMemoryNode(node1);

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      ApplicationNode appNode = new SimpleApplicationNode(directory, executor);

      // Initialize both keys to 100
      ByteBuffer buf0 = ByteBuffer.allocate(64);
      buf0.putInt(0, 100);
      MiniTransaction initTx0 = appNode.createMiniTransaction();
      initTx0.addWriteItem(new Item(0, 0, 0, buf0));
      assertTrue(appNode.executeTransaction(initTx0).isSuccess());

      ByteBuffer buf1 = ByteBuffer.allocate(64);
      buf1.putInt(0, 100);
      MiniTransaction initTx1 = appNode.createMiniTransaction();
      initTx1.addWriteItem(new Item(1, 0, 0, buf1));
      assertTrue(appNode.executeTransaction(initTx1).isSuccess());

      // Prepare T1: read-only multi-node transaction reading address 0 on Node 0 and Node 1
      MiniTransaction t1 = appNode.createMiniTransaction();
      node0.setT1TxId(t1.getTxId());
      node1.setT1TxId(t1.getTxId());

      ByteBuffer readBuf0 = ByteBuffer.allocate(64);
      Item readItem0 = new Item(0, 0, 0, readBuf0);
      t1.addReadItem(readItem0);

      ByteBuffer readBuf1 = ByteBuffer.allocate(64);
      Item readItem1 = new Item(1, 0, 0, readBuf1);
      t1.addReadItem(readItem1);

      // Prepare T2: transaction writing 200 to address 0 on Node 0 and Node 1
      MiniTransaction t2 = appNode.createMiniTransaction();
      ByteBuffer writeBuf0 = ByteBuffer.allocate(64);
      writeBuf0.putInt(0, 200);
      t2.addWriteItem(new Item(0, 0, 0, writeBuf0));

      ByteBuffer writeBuf1 = ByteBuffer.allocate(64);
      writeBuf1.putInt(0, 200);
      t2.addWriteItem(new Item(1, 0, 0, writeBuf1));

      // Start T1
      Future<Response> t1Future = executor.submit(() -> appNode.executeTransaction(t1));

      // Wait for T1 to successfully read Node 0
      assertTrue(t1ReadNode0Latch.await(5, TimeUnit.SECONDS), "Timeout waiting for T1 to read Node 0");

      // Execute T2
      Future<Response> t2Future = executor.submit(() -> appNode.executeTransaction(t2));
      Response t2Response = t2Future.get(5, TimeUnit.SECONDS);

      // Unblock T1 so it can continue and read Node 1
      t2CommitLatch.countDown();
      t1AllowedToReadNode1Latch.countDown();

      Response t1Response = t1Future.get(5, TimeUnit.SECONDS);

      if (t1Response.isSuccess() && t2Response.isSuccess()) {
        int val0 = readBuf0.getInt(0);
        int val1 = readBuf1.getInt(0);
        assertFalse(val0 == 100 && val1 == 200,
            "Fractured read detected: T1 read old value (100) from Node 0 and new value (200) from Node 1.");
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static class CoordinatedMemoryNode extends InMemoryMemoryNode {
    private final CountDownLatch t1ReadNode0Latch;
    private final CountDownLatch t2CommitLatch;
    private final CountDownLatch t1AllowedToReadNode1Latch;
    private String t1TxId;

    CoordinatedMemoryNode(int memoryNodeId, int addressSpace, int itemSize,
                          CountDownLatch t1ReadNode0Latch,
                          CountDownLatch t2CommitLatch,
                          CountDownLatch t1AllowedToReadNode1Latch) {
      super(memoryNodeId, addressSpace, itemSize);
      this.t1ReadNode0Latch = t1ReadNode0Latch;
      this.t2CommitLatch = t2CommitLatch;
      this.t1AllowedToReadNode1Latch = t1AllowedToReadNode1Latch;
    }

    void setT1TxId(String txId) {
      this.t1TxId = txId;
    }

    @Override
    public Result executeAndPrepare(MiniTransaction tx, Set<Integer> memoryNodeIds) {
      if (tx.getTxId().equals(t1TxId)) {
        if (getInfo().getId() == 0) {
          Result res = super.executeAndPrepare(tx, memoryNodeIds);
          t1ReadNode0Latch.countDown();
          try {
            t2CommitLatch.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return res;
        } else if (getInfo().getId() == 1) {
          try {
            t1AllowedToReadNode1Latch.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return super.executeAndPrepare(tx, memoryNodeIds);
        }
      }
      return super.executeAndPrepare(tx, memoryNodeIds);
    }
  }
}

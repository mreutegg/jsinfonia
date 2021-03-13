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
package org.apache.people.mreutegg.jsinfonia;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMemoryNode implements MemoryNode {

    private static final Logger log = LoggerFactory.getLogger(AbstractMemoryNode.class);

    private final Map<String, Locks> inDoubt = Collections.synchronizedMap(new HashMap<String, Locks>());

    private final Set<String> forcedAbort = Collections.synchronizedSet(new HashSet<String>());

    private final Map<Integer, Integer> readLocks = new HashMap<>();

    private final Set<Integer> writeLocks = new HashSet<>();

    private final MemoryNodeInfo info;

    protected AbstractMemoryNode(MemoryNodeInfo info) {
        this.info = info;
    }

    @Override
    public MemoryNodeInfo getInfo() {
        return info;
    }

    @Override
    public Result executeAndPrepare(MiniTransaction tx, Set<Integer> memoryNodeIds) {
        Result result;
        Set<ItemReference> failedCompares = new HashSet<>();
        Locks locks = createLocks(tx);
        getInDoubtMap().put(tx.getTxId(), locks);
        if (!acquireLocks(tx.getTxId(), locks)) {
            result = Result.BAD_LOCK;
        } else if (getForcedAbortSet().contains(tx.getTxId())) {
            result = Result.BAD_FORCED;
        } else if (!compareItemsMatch(tx, failedCompares).isEmpty()) {
            result = new Result(failedCompares);
        } else {
            result = Result.OK;
        }

        if (result.getVote() == Vote.OK) {
            try {
                readItems(tx);
                if (!tx.getWriteItems().isEmpty()) {
                    // only log transactions with write items
                    getRedoLog().append(tx.getTxId(), tx.getWriteItems(), memoryNodeIds);
                }
            } catch (IOException e) {
                result = Result.BAD_IO;
            }
        }

        if (result.getVote() != Vote.OK) {
            // abort now
            commit(tx.getTxId(), false, result.getVote() != Vote.BAD_LOCK);
            //System.out.println(vote);
        }
        return result;

    }

    @Override
    public void commit(String txId, boolean commit) {
        commit(txId, commit, true);
    }

    //-----------------------------< AbstractMemoryNode >----------------------

    protected void commit(String txId, boolean commit, boolean releaseLocks) {
        Locks locks = getInDoubtMap().remove(txId);
        if (locks == null) {
            // recovery coordinator executed first
            return;
        }
        if (getRedoLog().getTransactionIDs().contains(txId)) {
            // only transactions with write items need decision
            getRedoLog().decided(txId, commit);
        }
        if (releaseLocks) {
            releaseLocks(locks);
        }
    }

    protected Map<String, Locks> getInDoubtMap() {
        return inDoubt;
    }

    protected Set<String> getForcedAbortSet() {
        return forcedAbort;
    }

    protected Set<String> getInRedoLogSet() {
        return getRedoLog().getTransactionIDs();
    }

    protected Locks createLocks(MiniTransaction tx) {
        Locks locks = new Locks();
        for (Item item : tx.getWriteItems()) {
            locks.writeSet.add(item.getAddress());
        }
        for (Item item : tx.getCompareItems()) {
            if (!locks.writeSet.contains(item.getAddress())) {
                locks.readSet.add(item.getAddress());
            }
        }
        for (Item item : tx.getReadItems()) {
            if (!locks.writeSet.contains(item.getAddress())) {
                locks.readSet.add(item.getAddress());
            }
        }
        return locks;
    }

    protected boolean acquireLocks(String txId, Locks locks) {
        // synchronize on writeLocks
        synchronized (writeLocks) {
            if (Collections.disjoint(writeLocks, locks.readSet) &&
                    Collections.disjoint(writeLocks, locks.writeSet)) {
                log.debug("Acquire locks: writeSet={} readSet={}", locks.writeSet, locks.readSet);
                writeLocks.addAll(locks.writeSet);
                for (Integer address : locks.readSet) {
                    Integer i = readLocks.get(address);
                    if (i == null) {
                        i = 1;
                    } else {
                        i = i + 1;
                    }
                    readLocks.put(address, i);
                }
                return true;
            } else {
                return false;
            }
        }
    }

    protected void releaseLocks(Locks locks) {
        log.debug("Release locks: writeSet={} readSet={}", locks.writeSet, locks.readSet);
        // synchronize on writeLocks
        synchronized (writeLocks) {
            writeLocks.removeAll(locks.writeSet);
            for (Integer address : locks.readSet) {
                Integer i = readLocks.get(address);
                if (i != null) {
                    if (i == 1) {
                        readLocks.remove(address);
                    } else {
                        readLocks.put(address, i - 1);
                    }
                } else {
                    throw new IllegalStateException(
                            "No lock present for address: " + address);
                }
            }
        }
    }

    protected Set<ItemReference> compareItemsMatch(MiniTransaction tx, Set<ItemReference> failedCompares) {
        for (Item compareItem : tx.getCompareItems()) {
            ByteBuffer compareData = compareItem.getData();
            ByteBuffer data = ByteBuffer.allocate(compareData.remaining());
            try {
                readData(compareItem.getAddress(), compareItem.getOffset(), data);
            } catch (IOException e) {
                // exception while reading item
                log.warn("Error while reading item for compare", e);
                failedCompares.add(compareItem.getReference());
            }
            if (!data.equals(compareData)) {
                failedCompares.add(compareItem.getReference());
            }
        }
        return failedCompares;

    }

    protected void readItems(MiniTransaction tx) throws IOException {
        for (Item readItem : tx.getReadItems()) {
            readData(readItem.getAddress(), readItem.getOffset(), readItem.getData());
        }
    }
    
    protected void checkReadBuffer(ByteBuffer buffer, int offset)
            throws IllegalArgumentException {
        if (buffer.remaining() + offset > info.getItemSize()) {
            throw new IllegalArgumentException("Remaining buffer elements "
                    + buffer.remaining() + " + offset " + offset + " > "
                    + info.getItemSize());
        }
    }

    //---------------------------< subclass responsibility >-------------------

    protected abstract RedoLog getRedoLog();

    /**
     * Reads data at <code>address</code> with <code>offset</code> into the
     * given <code>buffer</code>. The number of bytes read into the buffer
     * is determined by <code>buffer.remaining()</code>.
     *
     * @param address the address to read.
     * @param offset the offset where to read.
     * @param buffer the destination buffer.
     * @throws IOException if an I/O error occurs while reading.
     */
    protected abstract void readData(int address, int offset, ByteBuffer buffer)
            throws IOException;

}

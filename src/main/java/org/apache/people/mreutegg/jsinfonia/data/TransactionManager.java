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
package org.apache.people.mreutegg.jsinfonia.data;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionManager implements TransactionContext {

    private static final Logger log = LoggerFactory.getLogger(TransactionManager.class);

    private final ApplicationNode appNode;

    private final DataItemCache readCache;

    private final DataItemCache writeCache;

    /**
     * Revisions of items read by the current transaction.
     * These revisions are compared on commit.
     */
    private Map<ItemReference, Integer> revisions = new HashMap<ItemReference, Integer>();

    public TransactionManager(ApplicationNode appNode,
            DataItemCache cache) {
        this.appNode = appNode;
        this.readCache = cache;
        this.writeCache = new DataItemCache();
    }

    public <T> T execute(Transaction<T> transaction) {
        int retries = 0;
        for (;;) {
            writeCache.clear();
            revisions.clear();
            if (retries++ > 0) {
                try {
                    Thread.sleep((long) (Math.random() * 2.0 * retries));
                } catch (InterruptedException e) {
                    // ignore
                    Thread.interrupted();
                }
            }
            log.debug("Start transaction");
            T result;
            try {
                result = transaction.perform(this);
            } catch (FailTransactionException e) {
                throw e.getCause();
            } catch (Exception e) {
                log.debug("Transaction failed");
                // evict read items used by current transaction
                readCache.evict(revisions.keySet());
                // retry
                continue;
            }

            /**
             * transaction where compare and write operations
             * are added to ensure consistent reads and writes.
             */
            MiniTransaction tx = appNode.createMiniTransaction();
            // Compare read versions
            for (Map.Entry<ItemReference, Integer> rev : revisions.entrySet()) {
                ByteBuffer b = ByteBuffer.allocate(DataItem.VERSION_LENGTH);
                b.putInt(0, rev.getValue());
                Item item = new Item(rev.getKey(), DataItem.VERSION_OFFSET, b);
                tx.addCompareItem(item);
            }
            for (ItemReference ref : writeCache.getKeys()) {
                DataItem dataItem = writeCache.getItem(ref);
                // every write has a preceding read
                int version = revisions.get(ref);
                // increment version for this write
                if (version == Integer.MAX_VALUE) {
                    version = 1;
                } else {
                    version++;
                }
                dataItem.setVersion(version);
                Item item = new Item(ref, 0, dataItem.asByteBuffer());
                tx.addWriteItem(item);
            }

            Response response = appNode.executeTransaction(tx);
            if (response.isSuccess()) {
                log.debug("Transaction successful");
                for (ItemReference ref : writeCache.getKeys()) {
                    // transfer all DataItems from writeCache to read cache
                    readCache.putItem(ref, writeCache.getItem(ref));
                }
                // and clear write cache
                writeCache.clear();
                return result;
            } else {
                log.debug("Transaction failed");
                readCache.evict(response.getFailedCompares());
                if (retries > 3) {
                    // if transaction failed more than three times
                    // we evict all items read by this transaction
                    readCache.evict(revisions.keySet());
                }
            }
            // if we get here we need to retry
        }
    }

    //-----------------------< TransactionContext >----------------------------

    @Override
    public <T> T read(ItemReference reference, DataOperation<T> op) {
        // perform read operation on the data
        if (log.isDebugEnabled()) {
            log.debug("read(" + reference + ")");
        }
        return op.perform(readItem(reference).getData().asReadOnlyBuffer());
    }

    @Override
    public <T> T write(ItemReference reference, DataOperation<T> op) {
        // perform write operation on the data
        if (log.isDebugEnabled()) {
            log.debug("write(" + reference + ")");
        }
        return op.perform(writeItem(reference).getData().duplicate());
    }

    //-----------------------------< internal >--------------------------------

    private DataItem writeItem(ItemReference reference) {
        // first check writeCache
        DataItem item = writeCache.getItem(reference);
        if (item != null) {
            return item;
        }
        // not yet modified, read it and put a copy in write cache
        item = new DataItem(readItem(reference));
        writeCache.putItem(reference, item);
        return item;
    }

    private DataItem readItem(ItemReference reference) {
        int memoryNodeId = reference.getMemoryNodeId();
        int address = reference.getAddress();
        // first check writeCache
        DataItem item = writeCache.getItem(reference);
        if (item != null) {
            return item;
        }
        item = readCache.getItem(reference);
        if (item == null) {
            // item is not in cache, need to read it
            MemoryNodeInfo info = appNode.getMemoryNodeInfos().get(memoryNodeId);
            if (info == null) {
                throw new IllegalArgumentException("No such MemoryNode: " + memoryNodeId);
            }
            Item i = new Item(memoryNodeId, address, 0, new byte[info.getItemSize()]);
            boolean success = false;
            while (success == false) {
                MiniTransaction mt = appNode.createMiniTransaction();
                mt.addReadItem(i);
                success = appNode.executeTransaction(mt).isSuccess();

                // TODO: copy data or use as is?
                item = new DataItem(i.getData());
            }
            readCache.putItem(reference, item);
        }

        // remember version for compare on commit
        revisions.put(reference, item.getVersion());
        return item;
    }
}

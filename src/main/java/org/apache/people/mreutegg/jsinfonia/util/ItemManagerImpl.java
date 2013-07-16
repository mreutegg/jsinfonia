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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.DataOperation;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;

/**
 * The <code>ItemManager</code> keeps track of items that are currently
 * in use by an application. It is the application responsibility to call
 * {@link #alloc()} and {@link #free(ItemReference)} accordingly.
 * <p/>
 * The <code>ItemManager</code> maintains an in-use <code>BitSet</code>
 * on every <code>MemoryNode</code>. Allocation of a new <code>Bucket</code>
 * is done transactionally by setting the respective bit in the in-use
 * <code>BitSet</code>. Similarly, {@link #free(ItemReference)} will
 * reset the respective bit to zero.
 * <p/>
 */
public class ItemManagerImpl implements ItemManager {

    private final TransactionContext txContext;

    private final ItemReference headerRef;

    private final FixedBitSet allocations;

    private byte[] zeros = null;

    /**
     * Creates a new <code>ItemManager</code> instance based on existing data
     * identified by the given <code>headerRef</code>.
     *
     * @param txContext the transaction context.
     * @param headerRef reference to the header item of this <code>ItemManager</code>.
     */
    public ItemManagerImpl(TransactionContext txContext, ItemReference headerRef) {
        this(txContext, headerRef, new FixedBitSet(txContext, headerRef));
    }

    private ItemManagerImpl(TransactionContext txContext, ItemReference headerRef, FixedBitSet allocations) {
        this.txContext = txContext;
        this.headerRef = headerRef;
        this.allocations = allocations;
    }

    /**
     * Initializes an <code>ItemManager</code> for the given <code>memoryNodeId</code>.
     *
     *
     * @param txContext
     * @param memoryNodeId
     * @return the header reference for the initialized item manager.
     */
    public static ItemReference initialize(TransactionContext txContext,
            int memoryNodeId, int addressSpace) {
        // find out what the item data size is
        int itemSize = txContext.read(new ItemReference(0, 0), new DataOperation<Integer>() {
            @Override
            public Integer perform(ByteBuffer data) {
                return data.remaining();
            }
        });
        int bitsPerItem = (itemSize / 8) * 64;
        // required number of dataItems to cover addressSpace
        int dataItems = addressSpace / bitsPerItem;
        if (addressSpace % bitsPerItem > 0) {
            dataItems++;
        }
        // cap at maximum number of item references the header can store
        dataItems = Math.min(dataItems, (itemSize - 8) / 12);
        List<ItemReference> dataItemRefs = new ArrayList<ItemReference>();
        for (int i = 0; i < dataItems; i++) {
            dataItemRefs.add(new ItemReference(memoryNodeId, i + 1));
        }

        ItemReference headerRef = new ItemReference(memoryNodeId, 0);
        ItemManagerImpl itemMgr = new ItemManagerImpl(txContext, headerRef,
                new FixedBitSet(txContext, headerRef, dataItemRefs, addressSpace));
        // the first item is the header
        itemMgr.allocations.set(0);
        // and allocations for bit set data items
        for (int i = 0; i < dataItems; i++) {
            itemMgr.allocations.set(i + 1);
        }

        return headerRef;
    }

    public ItemReference getHeaderReference() {
        return headerRef;
    }

    /* (non-Javadoc)
     * @see org.apache.mreutegg.jsinfonia.util.ItemManager#alloc()
     */
    @Override
    public ItemReference alloc() {
        // TODO: include some random? otherwise multiple threads will
        //        fight for the same next free item

        // TODO: improve? - nextClearBit() is O(N)
        int address = allocations.nextClearBit(0);
        if (address == -1) {
            return null;
        }
        allocations.set(address);
        ItemReference ref = new ItemReference(headerRef.getMemoryNodeId(), address);
        // clear data
        txContext.write(ref, new DataOperation<Void>() {
            @Override
            public Void perform(ByteBuffer data) {
                data.put(getZeros(data.remaining()), 0, data.remaining());
                return null;
            }
        });
        return ref;
    }

    /* (non-Javadoc)
     * @see org.apache.mreutegg.jsinfonia.util.ItemManager#free(org.apache.mreutegg.jsinfonia.ItemReference)
     */
    @Override
    public void free(ItemReference ref) {
        if (ref == null) {
            throw new NullPointerException();
        }
        if (ref.getMemoryNodeId() != headerRef.getMemoryNodeId()) {
            throw new IllegalArgumentException();
        }
        // flag as free
        allocations.clear(ref.getAddress());
    }

    //-------------------------------< internal >------------------------------

    private byte[] getZeros(int minSize) {
        if (zeros == null || zeros.length < minSize) {
            zeros = new byte[minSize];
        }
        return zeros;
    }
}

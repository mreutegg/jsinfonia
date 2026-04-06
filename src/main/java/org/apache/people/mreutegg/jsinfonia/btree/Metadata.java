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

import java.nio.ByteBuffer;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.DataOperation;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;

public class Metadata {

    private static final int MAGIC = 0x42545245; // "BTRE"

    private final TransactionContext txContext;
    private final ItemReference ref;

    public Metadata(TransactionContext txContext, ItemReference ref) {
        this.txContext = txContext;
        this.ref = ref;
    }

    public void initialize(final ItemReference rootNodeRef) {
        txContext.write(ref, data -> {
            data.putInt(MAGIC);
            data.putInt(rootNodeRef.getMemoryNodeId());
            data.putInt(rootNodeRef.getAddress());
            return null;
        });
    }

    public ItemReference getRootNodeRef() {
        return txContext.read(ref, data -> {
            if (data.getInt() != MAGIC) {
                throw new IllegalStateException("Invalid magic number");
            }
            int memoryNodeId = data.getInt();
            int address = data.getInt();
            return new ItemReference(memoryNodeId, address);
        });
    }

    public void setRootNodeRef(final ItemReference rootNodeRef) {
        txContext.write(ref, data -> {
            data.putInt(MAGIC);
            data.putInt(rootNodeRef.getMemoryNodeId());
            data.putInt(rootNodeRef.getAddress());
            return null;
        });
    }
}

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
import java.util.ArrayList;
import java.util.List;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.DataOperation;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;

public abstract class BTreeNode {

    public static final byte TYPE_INTERNAL = 0;
    public static final byte TYPE_LEAF = 1;

    protected final TransactionContext txContext;
    protected final ItemReference ref;
    protected final List<String> keys = new ArrayList<>();

    protected BTreeNode(TransactionContext txContext, ItemReference ref) {
        this.txContext = txContext;
        this.ref = ref;
    }

    public ItemReference getReference() {
        return ref;
    }

    public int getKeyCount() {
        return keys.size();
    }

    public String getKey(int index) {
        return keys.get(index);
    }

    public abstract void save();

    public abstract void load();

    public static BTreeNode load(final TransactionContext txContext, final ItemReference ref) {
        return txContext.read(ref, data -> {
            byte type = data.get();
            data.rewind();
            BTreeNode node;
            if (type == TYPE_INTERNAL) {
                node = new InternalNode(txContext, ref);
            } else if (type == TYPE_LEAF) {
                node = new LeafNode(txContext, ref);
            } else {
                throw new IllegalStateException("Unknown node type: " + type);
            }
            node.load();
            return node;
        });
    }

    protected void writeString(ByteBuffer buffer, String s) {
        byte[] bytes = s.getBytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    protected String readString(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }

    protected void writeBytes(ByteBuffer buffer, byte[] bytes) {
        if (bytes == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }
    }

    protected byte[] readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length == -1) {
            return null;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    protected void writeItemReference(ByteBuffer buffer, ItemReference ref) {
        buffer.putInt(ref.getMemoryNodeId());
        buffer.putInt(ref.getAddress());
    }

    protected ItemReference readItemReference(ByteBuffer buffer) {
        int memoryNodeId = buffer.getInt();
        int address = buffer.getInt();
        return new ItemReference(memoryNodeId, address);
    }
}

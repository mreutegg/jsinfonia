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

public class InternalNode extends BTreeNode {

    protected final List<ItemReference> children = new ArrayList<>();

    public InternalNode(TransactionContext txContext, ItemReference ref) {
        super(txContext, ref);
    }

    public ItemReference getChild(int index) {
        return children.get(index);
    }

    public void addChild(int index, ItemReference child) {
        children.add(index, child);
    }

    public void addKey(int index, String key) {
        keys.add(index, key);
    }

    public ItemReference removeChild(int index) {
        return children.remove(index);
    }

    public String removeKey(int index) {
        return keys.remove(index);
    }

    @Override
    public void save() {
        txContext.write(ref, new DataOperation<Void>() {
            @Override
            public Void perform(ByteBuffer data) {
                data.put(TYPE_INTERNAL);
                data.putInt(keys.size());
                for (String key : keys) {
                    writeString(data, key);
                }
                for (ItemReference child : children) {
                    writeItemReference(data, child);
                }
                return null;
            }
        });
    }

    @Override
    public void load() {
        txContext.read(ref, new DataOperation<Void>() {
            @Override
            public Void perform(ByteBuffer data) {
                byte type = data.get();
                if (type != TYPE_INTERNAL) {
                    throw new IllegalStateException("Not an internal node");
                }
                int count = data.getInt();
                keys.clear();
                for (int i = 0; i < count; i++) {
                    keys.add(readString(data));
                }
                children.clear();
                for (int i = 0; i <= count; i++) {
                    children.add(readItemReference(data));
                }
                return null;
            }
        });
    }
}

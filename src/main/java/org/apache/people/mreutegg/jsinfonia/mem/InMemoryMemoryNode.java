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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.people.mreutegg.jsinfonia.AbstractMemoryNode;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.RedoLog;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeInfo;


public class InMemoryMemoryNode extends AbstractMemoryNode {

    private final List<Item> items;

    private final InMemoryRedoLog redoLog = new InMemoryRedoLog(this);

    private final ByteBuffer empty;

    public InMemoryMemoryNode(int memoryNodeId, int addressSpace, int itemSize) {
        super(new SimpleMemoryNodeInfo(memoryNodeId, addressSpace, itemSize));
        items = new ArrayList<Item>(addressSpace);
        for (int i = 0; i < addressSpace; i++) {
            items.add(null);
        }
        this.empty = ByteBuffer.allocate(itemSize);
    }

    @Override
    protected RedoLog getRedoLog() {
        return redoLog;
    }

    @Override
    protected void readData(int address, int offset, ByteBuffer buffer)
            throws IOException {
        checkReadBuffer(buffer, offset);
        ByteBuffer data;
        Item item = items.get(address);
        if (item == null) {
            data = empty;
        } else {
            data = item.getData();
        }
        data = data.duplicate();
        data.position(offset);
        data.limit(offset + buffer.remaining());
        buffer.duplicate().put(data);
    }

    //-------------------------< InMemoryMemoryNode >--------------------------

    void applyWrites(List<Item> writeItems) {
        for (Item writeItem : writeItems) {
            Item item = items.get(writeItem.getAddress());
            if (item == null) {
                item = new Item(getInfo(), writeItem.getAddress());
                items.set(writeItem.getAddress(), item);
            }
            ByteBuffer dst = item.getData();
            dst.position(writeItem.getOffset());
            dst.put(writeItem.getData());
        }
    }
}

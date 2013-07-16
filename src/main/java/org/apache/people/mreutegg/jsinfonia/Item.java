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

import java.nio.ByteBuffer;

public class Item {

    private final int memoryNodeId;

    private final int address;

    private final int offset;

    private final ByteBuffer data;

    public Item(MemoryNodeInfo info, int address) {
        this(info.getId(), address, 0, new byte[info.getItemSize()]);
        if (address >= info.getAddressSpace()) {
            throw new IllegalArgumentException("Address: " + address +
                    ", AddressSpace: " + info.getAddressSpace());
        }
    }

    public Item(ItemReference itemRef, int offset, byte[] data) {
        this(itemRef.getMemoryNodeId(), itemRef.getAddress(), offset, ByteBuffer.wrap(data));
    }

    public Item(int memoryNodeId, int address, int offset, byte[] data) {
        this(memoryNodeId, address, offset, ByteBuffer.wrap(data));
    }

    public Item(ItemReference itemRef, int offset, ByteBuffer data) {
        this(itemRef.getMemoryNodeId(), itemRef.getAddress(), offset, data);
    }

    public Item(int memoryNodeId, int address, int offset, ByteBuffer data) {
        this.memoryNodeId = memoryNodeId;
        this.address = address;
        this.offset = offset;
        this.data = data.duplicate();
    }

    public int getMemoryNodeId() {
        return memoryNodeId;
    }

    public ByteBuffer getData() {
        return data.duplicate();
    }

    public int getAddress() {
        return address;
    }

    public int getOffset() {
        return offset;
    }

    public ItemReference getReference() {
        return new ItemReference(memoryNodeId, address);
    }
}

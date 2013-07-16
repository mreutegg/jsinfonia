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
package org.apache.people.mreutegg.jsinfonia.group;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.people.mreutegg.jsinfonia.Item;
import org.jgroups.Message;

abstract class MemoryNodeMessage extends Message {

    static final byte TYPE_COMMIT_MESSAGE = 0;

    static final byte TYPE_EXECUTE_AND_PREPARE_MESSAGE = 1;

    static final byte TYPE_RESULT_MESSAGE = 2;

    static final byte TYPE_GET_ADDRESS_SPACE_MESSAGE = 3;

    public MemoryNodeMessage() {
        setFlag(Message.Flag.DONT_BUNDLE);
    }

    abstract void accept(MemoryNodeMessageVisitor visitor) throws IOException;

    static MemoryNodeMessage fromBuffer(byte[] buffer) {
        MemoryNodeMessage mnm;
        switch (buffer[0]) {
            case TYPE_COMMIT_MESSAGE:
                mnm = CommitMessage.fromBuffer(buffer);
                break;
            case TYPE_EXECUTE_AND_PREPARE_MESSAGE:
                mnm = ExecuteAndPrepareMessage.fromBuffer(buffer);
                break;
            case TYPE_RESULT_MESSAGE:
                mnm = ResultMessage.fromBuffer(buffer);
                break;
            default:
                throw new IllegalStateException(
                        "Unknown message type: " + buffer[0]);
        }
        return mnm;
    }

    protected static int calculateSize(Item item, boolean emptyData) {
        int size = 4; // memoryNodeId
        size += 4; // address
        size += 4; // offset
        size += 4; // length
        if (!emptyData) {
            size += item.getData().remaining();
        }
        return size;
    }

    protected static Item readItem(ByteBuffer buffer, boolean emptyData) {
        int memoryNodeId = buffer.getInt();
        int address = buffer.getInt();
        int offset = buffer.getInt();
        int length = buffer.getInt();
        ByteBuffer data;
        if (emptyData) {
            data = ByteBuffer.allocate(length);
        } else {
            data = buffer.duplicate();
            data.limit(length + buffer.position());
            data = data.slice();
            buffer.position(buffer.position() + length);
        }
        return new Item(memoryNodeId, address, offset, data);
    }

    protected static void writeItem(Item item, ByteBuffer buffer, boolean emptyData) {
        buffer.putInt(item.getMemoryNodeId());
        buffer.putInt(item.getAddress());
        buffer.putInt(item.getOffset());
        buffer.putInt(item.getData().remaining());
        if (!emptyData) {
            buffer.put(item.getData().duplicate());
        }
    }
}
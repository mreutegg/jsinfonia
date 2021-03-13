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
import java.util.HashSet;
import java.util.Set;

import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;

import com.google.common.base.Preconditions;

public final class ExecuteAndPrepareMessage extends MemoryNodeMessage {

    private final MiniTransaction tx;
    private final Set<Integer> memoryNodeIds = new HashSet<>();

    private ExecuteAndPrepareMessage(MiniTransaction tx, Set<Integer> memoryNodeIds) {
        this.tx = tx;
        this.memoryNodeIds.addAll(memoryNodeIds);
    }

    public ExecuteAndPrepareMessage() {
        this.tx = null;
    }

    public static ExecuteAndPrepareMessage fromMiniTransaction(
            MiniTransaction tx, Set<Integer> memoryNodeIds) {
        ExecuteAndPrepareMessage msg = new ExecuteAndPrepareMessage(
                tx, memoryNodeIds);
        msg.setBuffer(msg.asByteBuffer().array());
        return msg;
    }

    public static ExecuteAndPrepareMessage fromBuffer(byte[] buffer) {
        ByteBuffer data = ByteBuffer.wrap(buffer);
        Preconditions.checkArgument(data.get() == TYPE_EXECUTE_AND_PREPARE_MESSAGE);
        StringBuilder txId = new StringBuilder();
        int len = data.getInt();
        for (int i = 0; i < len; i++) {
            txId.append(data.getChar());
        }
        MiniTransaction tx = new MiniTransaction(txId.toString());
        int numCompareItems = data.getInt();
        for (int i = 0; i < numCompareItems; i++) {
            tx.addCompareItem(readItem(data, false));
        }
        int numWriteItems = data.getInt();
        for (int i = 0; i < numWriteItems; i++) {
            tx.addWriteItem(readItem(data, false));
        }
        int numReadItems = data.getInt();
        for (int i = 0; i < numReadItems; i++) {
            tx.addReadItem(readItem(data, true));
        }
        Set<Integer> memoryNodeIds = new HashSet<>();
        int numMemoryNodeIds = data.getInt();
        for (int i = 0; i < numMemoryNodeIds; i++) {
            memoryNodeIds.add(data.getInt());
        }
        ExecuteAndPrepareMessage msg = new ExecuteAndPrepareMessage(
                tx, memoryNodeIds);
        msg.setBuffer(buffer);
        return msg;
    }

    public MiniTransaction getMiniTransaction() {
        return tx;
    }

    public Set<Integer> getMemoryNodeIds() {
        return memoryNodeIds;
    }

    //--------------------< MemoryNodeMessageVisitor >-------------------------

    @Override
    void accept(MemoryNodeMessageVisitor visitor) throws IOException {
        visitor.visit(this);
    }

    //-----------------------------< internal >--------------------------------

    private ByteBuffer asByteBuffer() {
        ByteBuffer data = ByteBuffer.wrap(new byte[calculateSize()]);
        data.put(TYPE_EXECUTE_AND_PREPARE_MESSAGE);
        String txId = tx.getTxId();
        data.putInt(txId.length());
        for (int i = 0; i < txId.length(); i++) {
            data.putChar(txId.charAt(i));
        }
        data.putInt(tx.getCompareItems().size());
        for (Item item : tx.getCompareItems()) {
            writeItem(item, data, false);
        }
        data.putInt(tx.getWriteItems().size());
        for (Item item : tx.getWriteItems()) {
            writeItem(item, data, false);
        }
        data.putInt(tx.getReadItems().size());
        for (Item item : tx.getReadItems()) {
            writeItem(item, data, true);
        }
        data.putInt(memoryNodeIds.size());
        for (Integer memoryNodeId : memoryNodeIds) {
            data.putInt(memoryNodeId);
        }
        return data;
    }

    private int calculateSize() {
        int size = 1; // type
        size += calculateSize(tx);
        size += calculateSize(memoryNodeIds);
        return size;
    }

    private static int calculateSize(MiniTransaction tx) {
        int size = tx.getTxId().length() * 2 + 4;
        size += 4; // numCompareItems (int)
        for (Item item : tx.getCompareItems()) {
            size += calculateSize(item, false);
        }
        size += 4; // numWriteItems (int)
        for (Item item : tx.getWriteItems()) {
            size += calculateSize(item, false);
        }
        size += 4; // numReadItems (int)
        for (Item item : tx.getReadItems()) {
            size += calculateSize(item, true);
        }
        return size;
    }

    private static int calculateSize(Set<Integer> memoryNodeIds) {
        int size = 4; // numMemoryNodeIds (int)
        size += memoryNodeIds.size() * 4;
        return size;
    }

}
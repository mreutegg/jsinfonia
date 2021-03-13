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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Result;
import org.apache.people.mreutegg.jsinfonia.Vote;

import com.google.common.base.Preconditions;

public final class ResultMessage extends MemoryNodeMessage {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final Result result;
    private final MiniTransaction tx;

    public ResultMessage() {
        this.result = null;
        this.tx = null;
    }

    private ResultMessage(Result result, MiniTransaction tx) {
        this.result = result;
        this.tx = tx;
    }

    static ResultMessage fromResult(Result result, MiniTransaction tx) {
        ResultMessage msg = new ResultMessage(result, tx);
        msg.setBuffer(msg.asBuffer().array());
        return msg;
    }

    static ResultMessage fromBuffer(byte[] buffer) {
        ByteBuffer data = ByteBuffer.wrap(buffer);
        Preconditions.checkArgument(data.get() == TYPE_RESULT_MESSAGE);
        byte[] txIdBytes = new byte[data.getInt()];
        data.get(txIdBytes);
        Vote vote = Vote.values()[data.getInt()];
        Result result;
        switch (vote) {
            case BAD_CMP:
                int memoryNodeId = -1;
                Set<ItemReference> failedCompares = new HashSet<>();
                int size = data.getInt();
                for (int i = 0; i < size; i++) {
                    if (i == 0) {
                        memoryNodeId = data.getInt();
                    }
                    ItemReference ref = new ItemReference(
                            memoryNodeId, data.getInt());
                    failedCompares.add(ref);
                }
                result = new Result(failedCompares);
                break;
            case BAD_FORCED:
                result = Result.BAD_FORCED;
                break;
            case BAD_IO:
                result = Result.BAD_IO;
                break;
            case BAD_LOCK:
                result = Result.BAD_LOCK;
                break;
            case OK:
                result = Result.OK;
                break;
            default:
                throw new IllegalArgumentException("Illegal vote: " + vote);
        }
        MiniTransaction mt = new MiniTransaction(new String(txIdBytes, UTF8));
        int numReadItems = data.getInt();
        for (int i = 0; i < numReadItems; i++) {
            mt.addReadItem(readItem(data, false));
        }
        ResultMessage msg = new ResultMessage(result, mt);
        msg.setBuffer(buffer);
        return msg;
    }

    Result getResult() {
        return result;
    }

    MiniTransaction getMiniTransaction() {
        return tx;
    }

    //-----------------------< MemoryNodeMessage >-----------------------------

    @Override
    void accept(MemoryNodeMessageVisitor visitor) throws IOException {
        visitor.visit(this);
    }

    //--------------------------< internal >-----------------------------------

    private ByteBuffer asBuffer() {
        int capacity = 1; // message type (byte)
        capacity += 4; // txId byte[] length (int)
        byte[] txIdBytes = tx.getTxId().getBytes(UTF8);
        capacity += txIdBytes.length;
        capacity += 4; // vote (int)
        if (result.getVote() == Vote.BAD_CMP) {
            capacity += 4; // failedCompares size (int)
            if (result.getFailedCompares().iterator().hasNext()) {
                capacity += 4; // memoryNodeId (int)
            }
            for (ItemReference ref : result.getFailedCompares()) {
                capacity += 4; // address (int)
            }
        }
        capacity += 4; // num read items
        for (Item item : tx.getReadItems()) {
            capacity += calculateSize(item, false);
        }
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(TYPE_RESULT_MESSAGE);
        buffer.putInt(txIdBytes.length);
        buffer.put(txIdBytes);
        buffer.putInt(result.getVote().ordinal());
        if (result.getVote() == Vote.BAD_CMP) {
            List<ItemReference> refs = new ArrayList<>();
            for (ItemReference ref : result.getFailedCompares()) {
                refs.add(ref);
            }
            buffer.putInt(refs.size());
            for (int i = 0; i < refs.size(); i++) {
                ItemReference ref = refs.get(i);
                if (i == 0) {
                    buffer.putInt(ref.getMemoryNodeId());
                }
                buffer.putInt(ref.getAddress());
            }
        }
        buffer.putInt(tx.getReadItems().size());
        for (Item item : tx.getReadItems()) {
            writeItem(item, buffer, false);
        }
        return buffer;
    }



}

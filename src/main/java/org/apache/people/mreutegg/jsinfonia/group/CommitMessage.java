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

import com.google.common.base.Preconditions;

public final class CommitMessage extends MemoryNodeMessage {

    private final String txId;
    private final boolean commit;

    public CommitMessage() {
        this.txId = null;
        this.commit = false;
    }

    private CommitMessage(String txId, boolean commit) {
        this.txId = txId;
        this.commit = commit;
    }

    static CommitMessage fromString(String txId, boolean commit) {
        CommitMessage msg = new CommitMessage(txId, commit);
        msg.setBuffer(msg.asByteBuffer().array());
        return msg;
    }

    static CommitMessage fromBuffer(byte[] buffer) {
        ByteBuffer data = ByteBuffer.wrap(buffer);
        Preconditions.checkArgument(data.get() == TYPE_COMMIT_MESSAGE);
        StringBuilder sb = new StringBuilder();
        int len = data.getInt();
        for (int i = 0; i < len; i++) {
            sb.append(data.getChar());
        }
        CommitMessage msg = new CommitMessage(
                sb.toString(), data.get() == 0);
        msg.setBuffer(buffer);
        return msg;
    }

    public String getTransactionId() {
        return txId;
    }

    public boolean isCommit() {
        return commit;
    }

    //------------------------< MemoryNodeMessage >----------------------------

    @Override
    void accept(MemoryNodeMessageVisitor visitor) throws IOException {
        visitor.visit(this);
    }

    ByteBuffer asByteBuffer() {
        ByteBuffer data = ByteBuffer.wrap(
                new byte[1 + 4 + 2 * txId.length() + 1]);
        data.put(TYPE_COMMIT_MESSAGE);
        data.putInt(txId.length());
        for (int i = 0; i < txId.length(); i++) {
            data.putChar(txId.charAt(i));
        }
        data.put(commit ? (byte) 0 : 1);
        return data;
    }

}
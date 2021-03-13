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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.RedoLog;


class InMemoryRedoLog implements RedoLog {

    private final Map<String, MiniTransaction> loggedTransactions = Collections.synchronizedMap(new HashMap<String, MiniTransaction>());

    private final InMemoryMemoryNode memoryNode;

    InMemoryRedoLog(InMemoryMemoryNode memoryNode) {
        this.memoryNode = memoryNode;
    }

    @Override
    public void append(String txId, List<Item> writeItems, Set<Integer> memoryNodeIds)
            throws IOException {
        MiniTransaction tx = new MiniTransaction(txId);
        for (Item writeItem : writeItems) {
            tx.addWriteItem(writeItem);
        }
        tx.getMemoryNodeIds().addAll(memoryNodeIds);
        loggedTransactions.put(txId, tx);
    }

    @Override
    public Set<String> getTransactionIDs() {
        Set<String> txIds = new HashSet<>();
        synchronized (loggedTransactions) {
            for (String txId : loggedTransactions.keySet()) {
                txIds.add(txId);
            }
        }
        return txIds;
    }

    @Override
    public void decided(String txId, boolean commit) {
        MiniTransaction tx = loggedTransactions.remove(txId);
        if (commit) {
            memoryNode.applyWrites(tx.getWriteItems());
        }
    }
}

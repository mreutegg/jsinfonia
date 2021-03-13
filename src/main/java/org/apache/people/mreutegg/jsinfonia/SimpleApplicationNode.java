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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.SettableFuture;

public class SimpleApplicationNode implements ApplicationNode {

    private static final Logger log = LoggerFactory.getLogger(SimpleApplicationNode.class);

    private final MemoryNodeDirectory<? extends MemoryNode> directory;

    private final ExecutorService executor;

    private static final AtomicLong txId = new AtomicLong();

    public SimpleApplicationNode(MemoryNodeDirectory<? extends MemoryNode> directory, ExecutorService executor) {
        this.directory = directory;
        this.executor = executor;
    }

    /* (non-Javadoc)
     * @see org.apache.mreutegg.jsinfonia.ApplicationNode#createMiniTransaction()
     */
    @Override
    public MiniTransaction createMiniTransaction() {
        return new MiniTransaction("" + txId.incrementAndGet());
    }

    @Override
    public Map<Integer, MemoryNodeInfo> getMemoryNodeInfos() {
        Map<Integer, MemoryNodeInfo> infos = new HashMap<>();
        for (int id : directory.getMemoryNodeIds()) {
            MemoryNode memoryNode = directory.getMemoryNode(id);
            infos.put(id, memoryNode.getInfo());
        }
        return infos;
    }

    /* (non-Javadoc)
     * @see org.apache.mreutegg.jsinfonia.ApplicationNode#executeTransaction(org.apache.mreutegg.jsinfonia.MiniTransaction)
     */
    @Override
    public Response executeTransaction(final MiniTransaction tx) {
        final Set<Integer> memoryNodeIds = tx.getMemoryNodeIds();
        List<Callable<Result>> callables = new ArrayList<>();
        for (final Integer memoryNodeId : memoryNodeIds) {
            final MiniTransaction mt = tx.getTransactionForMemoryNode(memoryNodeId);
            callables.add(new Callable<Result>() {
                @Override
                public Result call() throws Exception {
                    return directory.getMemoryNode(memoryNodeId).executeAndPrepare(mt, memoryNodeIds);
                }
            });
        }

        final Set<ItemReference> failedCompares = new HashSet<>();
        final boolean[] success = { false };
        try {
            List<? extends Future<Result>> results;
            if (callables.size() == 1) {
                // optimize single memory node case
                SettableFuture<Result> f = SettableFuture.create();
                try {
                    f.set(callables.get(0).call());
                } catch (Exception e) {
                    f.setException(e);
                }
                results = Collections.singletonList(f);
            } else {
                results = executor.invokeAll(callables);
            }
            boolean isVoteOK = true;
            for (Future<Result> result : results) {
                try {
                    Vote v = result.get().getVote();
                    isVoteOK &= v == Vote.OK;
                    if (v == Vote.BAD_CMP) {
                        for (ItemReference ref : result.get().getFailedCompares()) {
                            failedCompares.add(ref);
                        }
                    }
                } catch (ExecutionException e) {
                    log.warn("Exception on executeAndPrepare", e);
                    isVoteOK = false;
                }
            }
            success[0] = isVoteOK;
        } catch (InterruptedException e) {
            // success = false
            Thread.currentThread().interrupt();
        } catch (RejectedExecutionException e) {
            // success = false
        }
        callables.clear();
        for (final Integer memoryNodeId : memoryNodeIds) {
            callables.add(new Callable<Result>() {
                @Override
                public Result call() throws Exception {
                    MemoryNode mn = directory.getMemoryNode(memoryNodeId);
                    mn.commit(tx.getTxId(), success[0]);
                    return null;
                }
            });
        }
        if (callables.size() == 1) {
            // optimize single memory node
            if (tx.getWriteItems().isEmpty()) {
                // no commit needed when read-only
            } else {
                SettableFuture<Result> f = SettableFuture.create();
                try {
                    f.set(callables.get(0).call());
                } catch (Exception e) {
                    log.warn("Exception on commit", e);
                }
            }
        } else {
            try {
                executor.invokeAll(callables);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (success[0]) {
            return Response.SUCCESS;
        } else {
            return Response.failure(failedCompares);
        }
    }
}

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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Result;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.Vote;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.SettableFuture;

public class MemoryNodeGroupClient implements Closeable, MemoryNode, MemoryNodeMessageVisitor {

    private static final Logger log = LoggerFactory.getLogger(MemoryNodeGroupClient.class);

    private final ExecutorService executor;

    private final JChannel channel;

    private final int memoryNodeId;

    private MemoryNodeInfo info;

    private final Map<String, SettableFuture<ResultMessage>> pendingResults = Collections.synchronizedMap(
            new HashMap<String, SettableFuture<ResultMessage>>());

    public MemoryNodeGroupClient(final int memoryNodeId) throws Exception {
        this.executor =  Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicInteger numThreads = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, MemoryNodeGroupClient.class.getSimpleName() +
                        "-" + memoryNodeId + "-Thread-" + numThreads.getAndIncrement());
            }
        });
        this.memoryNodeId = memoryNodeId;
        this.channel = new JChannel("org/apache/people/mreutegg/jsinfonia/group/jgroups.xml");
        this.channel.setName("MemoryNodeGroupClient");
        this.channel.setReceiver(new CommunicationReceiver());
        this.channel.setDiscardOwnMessages(true);
        this.channel.connect("MemoryNodeGroup-" + memoryNodeId, null, 3L * 1000);
    }

    @Override
    public MemoryNodeInfo getInfo() {
        return info;
    }

    @Override
    public Result executeAndPrepare(final MiniTransaction tx,
            final Set<Integer> memoryNodeIds) {
        long time = System.currentTimeMillis();
        final SettableFuture<ResultMessage> future = SettableFuture.create();
        pendingResults.put(tx.getTxId(), future);
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        // TODO: send message to MemoryNodeGroupMembers only
                        channel.send(ExecuteAndPrepareMessage.fromMiniTransaction(
                                tx, memoryNodeIds));
                    } catch (Exception e) {
                        future.setException(e);
                    }
                }
            });
            ResultMessage r = future.get();
            if (r.getResult().getVote() == Vote.OK) {
                tx.getReadItems().clear();
                tx.getReadItems().addAll(r.getMiniTransaction().getReadItems());
            }
            return r.getResult();
        } catch (Exception e) {
            // FIXME: correct?
            return Result.BAD_IO;
        } finally {
            pendingResults.remove(tx.getTxId());
            time = System.currentTimeMillis() - time;
            log.debug("executeAndPrepare() of {} in {} ms", tx.getTxId(), time);
        }
    }

    @Override
    public void commit(final String txId, final boolean commit) {
        executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                // TODO: send message to MemoryNodeGroupMembers only
                channel.send(CommitMessage.fromString(txId, commit));
                return null;
            }
        });
    }

    //------------------------------< Closeable >------------------------------

    @Override
    public void close() {
        this.channel.close();
        this.executor.shutdown();
        while (!this.executor.isTerminated()) {
            try {
                this.executor.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore and wait again
                Thread.interrupted();
            }
        }
    }

    //--------------------< MemoryNodeMessageVisitor >-------------------------


    @Override
    public void visit(CommitMessage msg) throws IOException {
    }

    @Override
    public void visit(ExecuteAndPrepareMessage msg) throws IOException {
    }

    @Override
    public void visit(ResultMessage msg) throws IOException {
        SettableFuture<ResultMessage> future = pendingResults.get(msg.getMiniTransaction().getTxId());
        if (future != null) {
            future.set(msg);
        }
    }

    //----------------------------< internal >---------------------------------

    private final class CommunicationReceiver implements Receiver {

        @Override
        public void receive(Message msg) {
            try {
                MemoryNodeMessage.fromBuffer(msg.getBuffer()).accept(MemoryNodeGroupClient.this);
            } catch (IOException e) {
                log.warn("Exception on message receive", e);
            }
        }

        @Override
        public void getState(OutputStream output) throws Exception {
            DataOutputStream out = new DataOutputStream(output);
            out.writeInt(info.getAddressSpace());
            out.writeInt(info.getItemSize());
            out.flush();
        }

        @Override
        public void setState(InputStream input) throws Exception {
            DataInputStream in = new DataInputStream(input);
            int addressSpace = in.readInt();
            int itemSize = in.readInt();
            MemoryNodeGroupClient.this.info = new SimpleMemoryNodeInfo(
                    memoryNodeId, addressSpace, itemSize);
        }

        @Override
        public void viewAccepted(View new_view) {
            // TODO Auto-generated method stub

        }

        @Override
        public void suspect(Address suspected_mbr) {
            // TODO Auto-generated method stub

        }

        @Override
        public void block() {
            // TODO Auto-generated method stub

        }

        @Override
        public void unblock() {
            // TODO Auto-generated method stub

        }

    }
}

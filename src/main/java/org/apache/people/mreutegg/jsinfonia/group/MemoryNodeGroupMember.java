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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Result;
import org.apache.people.mreutegg.jsinfonia.Vote;
import org.apache.people.mreutegg.jsinfonia.util.ByteBufferInputStream;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Striped;

public class MemoryNodeGroupMember implements MemoryNodeMessageVisitor, Closeable {

	private static final Logger log = LoggerFactory.getLogger(MemoryNodeGroupMember.class);
	
	private static final String CHANNEL_NAME = "MemoryNodeGroupMember";
	
	private final ExecutorService executor;
	
	private final MemoryNode memoryNode;
	
	private final JChannel channel;
	
	private Map<String, ExecuteAndPrepareMessage> bufferedMessages = 
			Collections.synchronizedMap(new HashMap<String, ExecuteAndPrepareMessage>());
	
	private Striped<Lock> locks = Striped.lock(16);
	
	/**
	 * Address of the primary MemoryNodeGroupMember
	 */
	private Address primaryAddress;
	
	public MemoryNodeGroupMember(final MemoryNode memoryNode)
			throws Exception {
		this.executor =  Executors.newCachedThreadPool(new ThreadFactory() {
			private final AtomicInteger numThreads = new AtomicInteger();
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, MemoryNodeGroupMember.class.getSimpleName() + 
						"-" + memoryNode.getInfo().getId() + "-Thread-" + numThreads.getAndIncrement());
			}
		});
		this.memoryNode = memoryNode;
		this.channel = new JChannel("org/apache/people/mreutegg/jsinfonia/group/jgroups.xml");
		this.channel.setName(CHANNEL_NAME);
		this.channel.setDiscardOwnMessages(true);
		this.channel.setReceiver(new CommunicationReceiver());
		this.channel.connect("MemoryNodeGroup-" + memoryNode.getInfo().getId(), null, 60 * 1000, true);
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

	//----------------------< MemoryNodeMessageVisitor >-----------------------
	
	@Override
	public void visit(ExecuteAndPrepareMessage msg) throws IOException {
		if (channel.getAddress().equals(primaryAddress)) {
			executeAndPrepareOnPrimary(msg.getMiniTransaction(),
					msg.getMemoryNodeIds());
		} else {
			log.debug("Buffer ExecuteAndPrepareMessage {} on backup",
					msg.getMiniTransaction().getTxId());
			bufferedMessages.put(msg.getMiniTransaction().getTxId(), msg);
		}
	}

	@Override
	public void visit(CommitMessage msg) throws IOException {
		log.debug("Commit {}", msg.getTransactionId());
		Lock lock = locks.get(msg.getTransactionId());
		lock.lock();
		try {
			// it may happen that the commit message arrives on a backup
			// node before the result message. this can be detected by querying
			// the bufferedMessages map
			ExecuteAndPrepareMessage message = bufferedMessages.remove(msg.getTransactionId());
			if (message != null && msg.isCommit()) {
				executeAndPrepareOnBackup(message.getMiniTransaction(), message.getMemoryNodeIds());
			}
			memoryNode.commit(msg.getTransactionId(), msg.isCommit());
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void visit(ResultMessage msg) throws IOException {
		String txId = msg.getMiniTransaction().getTxId();
		Lock lock = locks.get(txId);
		lock.lock();
		try {
			ExecuteAndPrepareMessage message = bufferedMessages.remove(txId);
			if (message != null && msg.getResult().getVote() == Vote.OK) {
				executeAndPrepareOnBackup(message.getMiniTransaction(),
						message.getMemoryNodeIds());
			}
		} finally {
			lock.unlock();
		}
	}

	//----------------------------< internal >---------------------------------

	private void executeAndPrepareOnPrimary(MiniTransaction tx,
            Set<Integer> memoryNodeIds) {
		Result r = memoryNode.executeAndPrepare(tx, memoryNodeIds);
		log.debug("executeAndPrepareOnPrimary({}) {}", tx.getTxId(), r.getVote());
		sendMessage(ResultMessage.fromResult(r, tx));
	}
	
	private void executeAndPrepareOnBackup(MiniTransaction tx,
            Set<Integer> memoryNodeIds) throws IOException {
		// backup memory node only needs to apply writes
		tx.getCompareItems().clear();
		tx.getReadItems().clear();
		Result r;
		do {
			r = memoryNode.executeAndPrepare(tx, memoryNodeIds);
			log.debug("executeAndPrepareOnBackup({}) {}", tx.getTxId(), r.getVote());
			// FIXME: introduce random sleep on retry?
		} while (r.getVote() == Vote.BAD_LOCK); // retry if locks cannot be acquired
		if (r.getVote() != Vote.OK) {
			// the only expected type of bad vote is actually BAD_IO
			// BAD_CMP will not happen, because we cleared compare items
			// TODO: what about BAD_FORCED?
			throw new IOException("Bad vote: " + r.getVote());
		}
	}

	private void sendMessage(final MemoryNodeMessage msg) {
		executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					channel.send(msg);
				} catch (Exception e) {
					log.error("Unable to send message", e);
				}
			}
		});
	}
	
	//------------------------< Replication Receiver >-------------------------

	private final class CommunicationReceiver implements Receiver {

		@Override
		public void receive(Message msg) {
			try {
		        MemoryNodeMessage mnm = MemoryNodeMessage.fromBuffer(msg.getBuffer());
		        mnm.accept(MemoryNodeGroupMember.this);
	        } catch (IOException e) {
	        	// TODO: how to recover from this?
	        	channel.disconnect();
	        }
		}

		@Override
		public void getState(OutputStream output) throws Exception {
			DataOutputStream out = new DataOutputStream(output);
			MemoryNodeInfo info = memoryNode.getInfo();
			out.writeInt(info.getAddressSpace());
			out.writeInt(info.getItemSize());
			out.flush();
		}

		@Override
		public void setState(InputStream input) throws Exception {
			// TODO Auto-generated method stub
		}

		@Override
		public void viewAccepted(View new_view) {
			SortedSet<Address> sortedGroupMembers = new TreeSet<Address>();
			for (Address a : new_view.getMembers()) {
				if (a.toString().equals(CHANNEL_NAME)) {
					sortedGroupMembers.add(a);
				}
			}
			if (sortedGroupMembers.isEmpty()) {
				primaryAddress = null;
			} else {
				primaryAddress = sortedGroupMembers.iterator().next();
			}
		}

		@Override
		public void suspect(Address suspected_mbr) {
			log.info("suspect(" + suspected_mbr + ")");
		}

		@Override
		public void block() {
			// TODO: implement
			log.info("block()");
		}

		@Override
		public void unblock() {
			// TODO: implement
			log.info("unblock()");
		}
		
	}
	
	//------------------------< Replication Receiver >-------------------------
	
	// TODO: remove
	private final class ReplicationReceiver extends ReceiverAdapter {
		
		@Override
	    public void getState(OutputStream output) throws Exception {
			for (int i = 0; i < memoryNode.getInfo().getAddressSpace(); i++) {
				Item item = new Item(memoryNode.getInfo(), i);
				MiniTransaction tx = new MiniTransaction(UUID.randomUUID().toString());
				tx.addReadItem(item);
				Result r = memoryNode.executeAndPrepare(tx,
						Collections.singleton(memoryNode.getInfo().getId()));
				if (r.getVote() != Vote.OK) {
					throw new Exception("Error reading item " + i + ": " + r.getVote());
				}
				ByteBuffer data = item.getData();
				if (data.hasArray()) {
					output.write(data.array(), data.arrayOffset(), data.remaining());
				} else {
					ByteBufferInputStream in = new ByteBufferInputStream(data);
					ByteStreams.copy(in, output);
				}
			}
	    }

		@Override
	    public void setState(InputStream input) throws Exception {
			byte[] buffer = null;
			for (int i = 0; i < memoryNode.getInfo().getAddressSpace(); i++) {
				Item item = new Item(memoryNode.getInfo(), i);
				ByteBuffer data = item.getData();
				if (buffer == null) {
					buffer = new byte[data.remaining()];
				}
				ByteStreams.readFully(input, buffer);
				data.put(buffer);
				MiniTransaction tx = new MiniTransaction(UUID.randomUUID().toString());
				tx.addWriteItem(item);
				Result r = memoryNode.executeAndPrepare(tx, 
						Collections.singleton(memoryNode.getInfo().getId()));
				if (r.getVote() != Vote.OK) {
					throw new Exception("Error writing item " + i + ": " + r.getVote());
				}
				memoryNode.commit(tx.getTxId(), true);
			}
	    }
	}
	
	

}

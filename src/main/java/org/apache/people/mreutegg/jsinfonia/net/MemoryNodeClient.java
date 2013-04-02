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
package org.apache.people.mreutegg.jsinfonia.net;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.people.mreutegg.jsinfonia.thrift.MemoryNodeService.Client;
import org.apache.people.mreutegg.jsinfonia.thrift.TMemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.thrift.TResult;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Result;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeInfo;

public class MemoryNodeClient extends ThriftClient<Client> implements MemoryNode {

	private static final Logger log = LoggerFactory.getLogger(MemoryNodeClient.class);

	private final MemoryNodeInfo info;
	
	public MemoryNodeClient(String host, int port, int numConnections, boolean framed)
			throws TException {
		super(host, port, numConnections, framed);
		TMemoryNodeInfo info = executeWithClient(new ClientCallable<TMemoryNodeInfo, Client, TException>() {
			@Override
			public TMemoryNodeInfo call(Client client) throws TException {
				return client.getInfo();
			}
		}); 
		this.info = new SimpleMemoryNodeInfo(info.getId(),
				info.getAddressSpace(), info.getItemSize());
	}
	

	@Override
    protected Client createClient(TTransport transport) {
		return new Client(new TBinaryProtocol(transport));
    }

	@Override
	public MemoryNodeInfo getInfo() {
		return info;
	}

	@Override
	public Result executeAndPrepare(final MiniTransaction tx, final Set<Integer> memoryNodeIds) {
		try {
			return executeWithClient(new ClientCallable<Result, Client, TException>() {
				@Override
				public Result call(Client client) throws TException {
					TResult result = client.executeAndPrepare(Utils.convert(tx), memoryNodeIds);
					List<Item> readItems = tx.getReadItems();
					for (int i = 0; i < result.getReadItemsSize(); i++) {
						ByteBuffer data = readItems.get(i).getData();
						ByteBuffer buffer = result.getReadItems().get(i).bufferForData().slice();
						data.put(buffer);
					}
					return Utils.convert(result);
				}
			});
		} catch (TException e) {
			log.warn("exception on executeAndPrepare", e);
			return Result.BAD_IO;
		}
	}

	@Override
	public void commit(final String txId, final boolean commit) {
		try {
			executeWithClient(new ClientCallable<Void, Client, TException>() {
				@Override
				public Void call(Client client) throws TException {
					client.commit(txId, commit);
					return null;
				}
			});
		} catch (TException e) {
			// TODO: this requires a 'recovery coordinator'
			// see sinfonia paper section 4.4
			log.warn("unable to commit", e);
		}
	}
}

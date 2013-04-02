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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.people.mreutegg.jsinfonia.thrift.ApplicationNodeService.Client;
import org.apache.people.mreutegg.jsinfonia.thrift.TMemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.thrift.TResponse;
import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Response;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeInfo;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

public class ApplicationNodeClient extends ThriftClient<Client> implements ApplicationNode {

	public ApplicationNodeClient(String host, int port, int numConnections, boolean framed)
			throws TException {
	    super(host, port, numConnections, framed);
    }

	@Override
    protected Client createClient(TTransport transport) {
		return new Client(new TBinaryProtocol(transport));
    }

	//--------------------------------< ApplicationNode >----------------------
	
	@Override
    public MiniTransaction createMiniTransaction() {
	    return new MiniTransaction(UUID.randomUUID().toString());
    }

	@Override
    public Map<Integer, MemoryNodeInfo> getMemoryNodeInfos() {
		try {
	        List<TMemoryNodeInfo> infos = executeWithClient(
	        		new ClientCallable<List<TMemoryNodeInfo>, Client, TException>() {
	        	@Override
	            public List<TMemoryNodeInfo> call(Client client) throws TException {
	                return client.getMemoryNodeInfos();
	            }
	        });
	        Map<Integer, MemoryNodeInfo> map = new HashMap<Integer, MemoryNodeInfo>();
	        for (TMemoryNodeInfo info : infos) {
	        	map.put(info.getId(), new SimpleMemoryNodeInfo(
	        			info.getId(), info.getAddressSpace(), info.getItemSize()));
	        }
	        return map;
	        
        } catch (TException e) {
        	throw new RuntimeException(e);
        }
    }

	@Override
    public Response executeTransaction(final MiniTransaction tx) {
		try {
			TResponse response = executeWithClient(
					new ClientCallable<TResponse, Client, TException>() {
				@Override
				public TResponse call(Client client) throws TException {
					return client.executeTransaction(Utils.convert(tx));
				}
			});
			return Utils.convert(response);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
    }
}

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

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

public abstract class ThriftServer {

    private final int port;

    private final boolean nonBlocking;

    private int usedPort;

    private TServer server;

    private TServerTransport transport;

    public ThriftServer(int port, boolean nonBlocking) {
        this.port = port;
        this.nonBlocking = nonBlocking;
    }

    public void start() throws TTransportException {
        createServer();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                server.serve();
            }
        });
        t.start();
        while (!server.isServing()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        t.setName("MemoryNodeServer on port " + usedPort);
    }

    public int getPort() {
        return usedPort;
    }

    public void stop() {
        if (server != null && server.isServing()) {
            server.stop();
            server = null;
        }
        if (transport != null) {
            transport.close();
            transport = null;
        }
    }

    protected abstract TProcessor createProcessor();

    //---------------------------< internal >----------------------------------

    private void createServer() throws TTransportException {
        if (port >= 0) {
            while (transport == null) {
                usedPort = (int) (Math.random() * 32000d) + 32000;
                try {
                    if (nonBlocking) {
                        transport = new TNonblockingServerSocket(usedPort);
                    } else {
                        transport = new TServerSocket(usedPort);
                    }
                } catch (TTransportException e) {
                    // try another one
                }
            }
        } else {
            if (nonBlocking) {
                transport = new TNonblockingServerSocket(port);
            } else {
                transport = new TServerSocket(usedPort);
            }
            usedPort = port;
        }
        TProcessor processor = createProcessor();
        if (nonBlocking) {
            server = new TThreadedSelectorServer(
                    new TThreadedSelectorServer
                    .Args((TNonblockingServerSocket) transport)
                    .transportFactory(new TFramedTransport.Factory())
                    .processor(processor));
        } else {
            server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(transport)
                    .processor(processor));
        }
    }
}

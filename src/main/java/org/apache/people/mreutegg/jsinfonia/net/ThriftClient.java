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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ThriftClient<C> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ThriftClient.class);

    private final List<TSocket> sockets = new ArrayList<>();
    private final BlockingQueue<C> clients = new LinkedBlockingQueue<>();
    private final int numConnections;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ThriftClient(String host, int port, int numConnections, boolean framed)
            throws TException {
        this.numConnections = numConnections;
        for (int i = 0; i < numConnections; i++) {
            TSocket socket = new TSocket(host, port);
            sockets.add(socket);
            TTransport transport = socket;
            if (framed) {
                transport = new TFramedTransport(socket);
            }
            transport.open();
            log.debug("opened socket to {}:{} on {}", new Object[]{host, port, socket.getSocket().getLocalPort()});
            clients.add(createClient(transport));
        }
    }

    protected abstract C createClient(TTransport transport);

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            for (int i = 0; i < numConnections; i++) {
                // drain clients
                boolean success = false;
                while (!success) {
                    try {
                        clients.take();
                        success = true;
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                    }
                }
            }
            // close sockets
            for (TSocket s : sockets) {
                s.close();
            }
        }
    }

    //-------------------------------< internal >------------------------------

    protected <T, E extends Throwable> T executeWithClient(final ClientCallable<T, C, E> callable) throws E {
        checkOpen();
        C c = null;
        while (c == null) {
            try {
                c = clients.take();
            } catch (InterruptedException e) {
                Thread.interrupted();
                // and try again
            }
        }
        try {
            return callable.call(c);
        } finally {
            clients.add(c);
        }
    }

    protected interface ClientCallable<T, C, E extends Throwable> {

        T call(C client) throws E;

    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("closed");
        }
    }

}

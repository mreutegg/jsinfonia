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
package org.apache.people.mreutegg.jsinfonia.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.data.DataOperation;
import org.apache.people.mreutegg.jsinfonia.data.Transaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionTest extends AbstractTransactionTest {

    private static final Logger log = LoggerFactory.getLogger(TransactionTest.class);

    private static final int NUM_WORKERS = 10;

    private static final int COUNT_TO = 1000;

    @Override
    protected MemoryNodeDirectory<? extends MemoryNode> createDirectory()
            throws IOException {
        return createDirectory(1, 1024, 1024, 128);
    }

    public void testTransaction() throws Exception {

        List<Integer> results = Collections.synchronizedList(new ArrayList<Integer>());
        List<Thread> workers = new ArrayList<Thread>();
        for (int i = 0; i < NUM_WORKERS; i++) {
            workers.add(new Thread(new Worker(createTransactionContext(), results)));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
        int sum = 0;
        for (Integer i : results) {
            sum += i;
        }
        assertEquals(COUNT_TO, sum);
    }

    private static class Worker implements Runnable {

        private final TransactionManager context;
        private final List<Integer> results;

        Worker(TransactionManager context, List<Integer> results) {
            this.context = context;
            this.results = results;
        }

        @Override
        public void run() {
            int numIncrements = 0;
            boolean incremented;
            do {
                incremented = context.execute(new Transaction<Boolean>() {
                    @Override
                    public Boolean perform(TransactionContext txContext) {
                        final int value = txContext.read(new ItemReference(0, 0), new DataOperation<Integer>() {
                            @Override
                            public Integer perform(ByteBuffer data) {
                                return data.getInt(0);
                            }
                        });
                        log.debug("read " + value);
                        if (value < COUNT_TO) {
                            txContext.write(new ItemReference(0, 0), new DataOperation<Void>() {
                                @Override
                                public Void perform(ByteBuffer data) {
                                    data.putInt(0, value + 1);
                                    return null;
                                }

                            });
                            log.debug("write " + (value + 1));
                            return true;
                        } else {
                            return false;
                        }
                    }
                });
                if (incremented) {
                    numIncrements++;
                }
            } while (incremented);
            results.add(numIncrements);
        }
    }
}

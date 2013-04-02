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

public class DistributedTransactionTest extends AbstractTransactionTest {

	private static final Logger log = LoggerFactory.getLogger(DistributedTransactionTest.class);
	
	private static final int NUM_WORKERS = 10;
	
	private static final int INITIAL_COUNT = 1000;

	@Override
	protected MemoryNodeDirectory<? extends MemoryNode> createDirectory()
			throws IOException {
		return createDirectory(2, 1024, 1024, 128);
	}

	public void testTransaction() throws Exception {
		initData();
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
		verifyData();
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
			int transferred = 0;
			int transfer;
			do {
				transfer = context.execute(new Transaction<Integer>() {
					@Override
					public Integer perform(TransactionContext txContext) {
						final int v = 1 + (int) (Math.random() * 2.0);
						final int v0 = txContext.read(new ItemReference(0, 0), new DataOperation<Integer>() {
							@Override
							public Integer perform(ByteBuffer data) {
								return data.getInt(0);
							}
						});
						log.debug("read(0) " + v0);
						if (v0 == 0) {
							return -1;
						}
						if (v0 < v) {
							return 0;
						}
						txContext.write(new ItemReference(0, 0), new DataOperation<Void>() {
							@Override
							public Void perform(ByteBuffer data) {
								data.putInt(0, v0 - v);
								return null;
							}
							
						});
						log.debug("write(0) " + (v0 - v));

						final int v1 = txContext.read(new ItemReference(1, 0), new DataOperation<Integer>() {
							@Override
							public Integer perform(ByteBuffer data) {
								return data.getInt(0);
							}
						});
						log.debug("read(1) " + v1);
						txContext.write(new ItemReference(1, 0), new DataOperation<Void>() {
							@Override
							public Void perform(ByteBuffer data) {
								data.putInt(0, v1 + v);
								return null;
							}
							
						});
						log.debug("write(1) " + (v1 + v));
						return v;
					}
				});
				if (transfer > 0) {
					transferred += transfer;
				}
			} while (transfer >= 0);
			results.add(transferred);
		}
	}

	private void initData() {
		TransactionManager context = createTransactionContext();
		context.execute(new Transaction<Void>() {
			@Override
			public Void perform(TransactionContext txContext) {
				txContext.write(new ItemReference(0, 0), new DataOperation<Void>() {
					@Override
					public Void perform(ByteBuffer data) {
						data.putInt(0, INITIAL_COUNT);
						return null;
					}
				});
				return null;
			}
		});
	}

	private void verifyData() {
		TransactionManager context = createTransactionContext();
		assertEquals((Integer) 0, context.execute(new Transaction<Integer>() {
			@Override
			public Integer perform(TransactionContext txContext) {
				return txContext.read(new ItemReference(0, 0), new DataOperation<Integer>() {
					@Override
					public Integer perform(ByteBuffer data) {
						return data.getInt(0);
					}
				});
			}
		}));
		assertEquals((Integer) INITIAL_COUNT, context.execute(new Transaction<Integer>() {
			@Override
			public Integer perform(TransactionContext txContext) {
				return txContext.read(new ItemReference(1, 0), new DataOperation<Integer>() {
					@Override
					public Integer perform(ByteBuffer data) {
						return data.getInt(0);
					}
				});
			}
		}));
	}
}

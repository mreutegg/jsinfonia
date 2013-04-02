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
package org.apache.people.mreutegg.jsinfonia.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ItemBuffer implements Runnable {
	
	private static final Logger log = LoggerFactory.getLogger(ItemBuffer.class);

	private final FileMemoryNode memoryNode;
	
	private final int capacity;
	
	private final int itemSize;
	
	private final Object monitor = new Object();
	
	private final Map<Integer, CachedItem> dirtyItems = 
			new LinkedHashMap<Integer, CachedItem>();
	
	private final Thread writeBackThread;
	
	private final AtomicBoolean running = new AtomicBoolean(true);
	
	ItemBuffer(FileMemoryNode memoryNode, int capacity, int itemSize) {
		this.memoryNode = memoryNode;
		this.capacity = capacity;
		this.itemSize = itemSize;
		this.writeBackThread = new Thread(null, this, "WriteBackThread");
		this.writeBackThread.start();
	}
	
	void close() {
		synchronized (monitor) {
			while (!dirtyItems.isEmpty()) {
				try {
	                monitor.wait();
                } catch (InterruptedException e) {
                	// retry
                }
			}
		}
		running.set(false);
		this.writeBackThread.interrupt();
		try {
	        this.writeBackThread.join();
        } catch (InterruptedException e) {
        	log.warn("Interrupted while waiting for write back thread to finish");
        }
	}
	
	
    void applyWrite(ReadableByteChannel data, int address, int offset, int count)
    		throws IOException {
    	boolean locked = false;
    	CachedItem item = null;
    	try {
	        synchronized (monitor) {
	        	// respect capacity
	        	while (dirtyItems.size() >= capacity) {
	        		try {
	                    monitor.wait();
                    } catch (InterruptedException e) {
                    	// retry
                    }
	        	}
	        	item = dirtyItems.get(address); 
	        }
	        if (item == null) {
	        	ByteBuffer buffer = ByteBuffer.allocate(itemSize);
	        	memoryNode.readData(address, 0, buffer);
	        	item = new CachedItem(buffer);
	        }
	        item.lock();
	        locked = true;
	        // apply write
	        ByteBuffer dest = item.asByteBuffer();
	        dest.position(offset);
	        dest.limit(offset + count);
	        do {
	        	data.read(dest); 
	        } while (dest.remaining() > 0);
	        // put back
	        synchronized (monitor) {
	        	dirtyItems.put(address, item);
	        	monitor.notifyAll();
	        }
        } finally {
        	if (item != null && locked) {
        		item.unlock();
        	}
        }
    }
    
    ByteBuffer getItem(int address) {
    	synchronized (monitor) {
	        CachedItem item = dirtyItems.get(address);
	        return item != null ? item.asByteBuffer() : null;
        }
    }
    
    //--------------------------< Runnable >-----------------------------------

	@Override
    public void run() {
		long numWriteBacks = 0;
		while (running.get()) {
			int address;
			CachedItem item;
			synchronized (monitor) {
				try {
					while (dirtyItems.isEmpty()) {
	                    monitor.wait();
					}
                } catch (InterruptedException e) {
                	// still running?
                	continue;
                }
				// first entry is least recently used
				Entry<Integer, CachedItem> entry = dirtyItems.entrySet().iterator().next();
				address = entry.getKey();
				item = entry.getValue();
			}
			item.lock();
			try {
				numWriteBacks++;
	            memoryNode.applyWrite(item.asByteBuffer(), address);
	            if (numWriteBacks % 1000 == 0) {
	            	// sync every 1000 writes
	            	// memoryNode.sync();
	            }
	            // success
	            synchronized (monitor) {
	            	dirtyItems.remove(address);
	            	monitor.notifyAll();
                }
            } catch (IOException e) {
            	log.warn("Unable to write back dirty item", e);
            	try {
            		// sleep for a second to avoid excessive warnings
	                Thread.sleep(1000);
                } catch (InterruptedException ex) {
                	// ignore
                }
			} finally {
				item.unlock();
			}
		}
		System.out.println("numWriteBacks: " + numWriteBacks);
    }
}

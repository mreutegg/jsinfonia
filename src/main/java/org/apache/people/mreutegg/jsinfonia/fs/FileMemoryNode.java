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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import org.apache.people.mreutegg.jsinfonia.AbstractMemoryNode;
import org.apache.people.mreutegg.jsinfonia.RedoLog;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileMemoryNode extends AbstractMemoryNode implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(FileMemoryNode.class);
	
	private final RandomAccessFile file;
	
	private final FileChannel channel;
	
	private final RollingRedoLog redoLog;
	
	private final ItemBuffer itemBuffer;
	
	public FileMemoryNode(int memoryNodeId, File file, int addressSpace, int itemSize, int bufferSize)
			throws IOException {
	    super(new SimpleMemoryNodeInfo(memoryNodeId, addressSpace, itemSize));
	    if (!file.exists() && !file.createNewFile()) {
	    	throw new IOException("Unable to create file: " + file.getAbsolutePath());
	    }
	    	
	    this.file = new RandomAccessFile(file, "rw");
	    if (this.file.length() < ((long) addressSpace) * ((long) itemSize)) {
	    	this.file.setLength(((long) addressSpace) * ((long) itemSize));
	    }
	    this.channel = this.file.getChannel();
	    this.itemBuffer = new ItemBuffer(this, bufferSize, itemSize);
	    this.redoLog = new RollingRedoLog(this, new File(file.getAbsoluteFile() + ".log"));
    }

	@Override
	protected RedoLog getRedoLog() {
		return redoLog;
	}

	@Override
    protected void readData(int address, int offset, ByteBuffer buffer)
            throws IOException {
		checkReadBuffer(buffer, offset);
		// check buffer for dirty items first
		ByteBuffer src = itemBuffer.getItem(address);
		if (src != null) {
			src.position(offset);
			src.limit(offset + buffer.remaining());
			buffer.duplicate().put(src.slice());
			return;
		}
		// otherwise read from channel
		ByteBuffer dest = buffer.duplicate();
    	long position = ((long) address) * getInfo().getItemSize() + offset;
		int numRead = 0;
    	while (dest.remaining() > 0 && numRead >= 0) {
	    	numRead = this.channel.read(dest, position);
		}
    }


	//--------------------------< Closeable >----------------------------------

	@Override
    public void close() throws IOException {
		this.redoLog.close();
		this.itemBuffer.close();
		this.channel.close();
		this.file.close();
    }
	
	//------------------------< FileMemoryNode >-------------------------------

    void applyWrite(ByteBuffer data, int address)
    		throws IOException {
		long position = ((long) address) * getInfo().getItemSize();
    	do {
    		position += this.channel.write(data, position);
    	} while (data.hasRemaining());
    }
    
	void applyWrite(ReadableByteChannel data, int address, int offset, int count)
			throws IOException {
	    itemBuffer.applyWrite(data, address, offset, count);
    }
	
    public void sync() throws IOException {
    	long time = System.currentTimeMillis();
    	this.channel.force(true);
    	time = System.currentTimeMillis() - time;
    	log.info("sync time: {} ms.", time);
    }

}

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

import java.nio.ByteBuffer;

public class DataItem {
	
	public static final byte TYPE_VERSION = 0;
	
	public static final byte TYPE_VERSION_REPLICATED = 1;
	
	public static final int VERSION_OFFSET = 0;
	
	public static final int VERSION_LENGTH = 4;
	
	public static final int TYPE_OFFSET = 4;
	
	public static final int TYPE_LENGTH = 1;
	
	public static final int META_LENGTH = VERSION_LENGTH + TYPE_LENGTH;
	
	private final ByteBuffer data;
	
	public DataItem(byte[] data) {
		this.data = ByteBuffer.wrap(data);
	}
	
	/**
	 * Creates a new DataItem based on the given <code>item</code>.
	 * The underlying data is copied.
	 * 
	 * @param item the DataItem to copy.
	 */
	public DataItem(DataItem item) {
		if (item.data.isDirect()) {
			this.data = ByteBuffer.allocateDirect(item.data.remaining());
		} else {
			this.data = ByteBuffer.allocate(item.data.remaining());
		}
		this.data.put(item.data.duplicate());
		this.data.rewind();
	}
	
	public DataItem(ByteBuffer data) {
		this.data = data.slice();
	}

	public int getVersion() {
		return data.getInt(0);
	}
	
	public void setVersion(int version) {
		data.putInt(0, version);
	}
	
	public byte getType() {
		return data.get(4);
	}
	
	public void setType(byte type) {
		data.put(4, type);
	}
	
	/**
	 * Returns a the data of this item as a ByteBuffer. Modifications
	 * to the returned ByteBuffer will be written back to this DataItem.
	 * 
	 * @return the data of this item as a ByteBuffer.
	 */
	public ByteBuffer getData() {
		ByteBuffer dup = data.duplicate();
		dup.position(META_LENGTH);
		return dup.slice();
	}
	
	/**
	 * Returns the backing <code>ByteBuffer</code> of this <code>DataItem</code>.
	 * This includes the complete item with type, version and data bytes.
	 * 
	 * @return the backing <code>ByteBuffer</code>.
	 */
	public ByteBuffer asByteBuffer() {
		return data.duplicate();
	}
}

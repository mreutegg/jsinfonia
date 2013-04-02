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
package org.apache.people.mreutegg.jsinfonia;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ItemReference {

	private final int memoryNodeId;
	
	private final int address;
	
	public ItemReference(int memoryNodeId, int address) {
		this.memoryNodeId = memoryNodeId;
		this.address = address;
	}
	
	public int getMemoryNodeId() {
    	return memoryNodeId;
    }

	public int getAddress() {
    	return address;
    }
	
	public static ItemReference readFrom(DataInputStream in)
			throws IOException {
		return new ItemReference(in.readInt(), in.readInt());
	}
	
	public void writeTo(DataOutputStream out) throws IOException {
		out.writeInt(memoryNodeId);
		out.writeInt(address);
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ItemReference(");
		sb.append(memoryNodeId);
		sb.append(", ");
		sb.append(address);
		sb.append(")");
		return sb.toString();
	}

	@Override
    public int hashCode() {
		int hash = 17;
		hash = 37 * hash + memoryNodeId;
		hash = 37 * hash + address;
        return hash;
    }

	@Override
    public boolean equals(Object obj) {
		if (obj instanceof ItemReference) {
			ItemReference other = (ItemReference) obj;
			return memoryNodeId == other.memoryNodeId && address == other.address;
		} else {
			return false;
		}
    }
}

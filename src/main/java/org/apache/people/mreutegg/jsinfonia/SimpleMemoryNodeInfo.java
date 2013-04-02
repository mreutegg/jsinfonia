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

public class SimpleMemoryNodeInfo implements MemoryNodeInfo {

	private final int id;
	private final int addressSpace;
	private final int itemSize;
	
	public SimpleMemoryNodeInfo(int id, int addressSpace, int itemSize) {
		this.id = id;
		this.addressSpace = addressSpace;
		this.itemSize = itemSize;
	}
	
	@Override
	public int getId() {
		return id;
	}

	@Override
	public int getAddressSpace() {
		return addressSpace;
	}

	@Override
	public int getItemSize() {
		return itemSize;
	}
}

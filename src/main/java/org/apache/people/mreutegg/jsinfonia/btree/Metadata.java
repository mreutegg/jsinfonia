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
package org.apache.people.mreutegg.jsinfonia.btree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.util.ByteBufferInputStream;


public class Metadata {

	private static final int MAGIC = 1029384756;
	
	private final TransactionContext txContext;
	
	private int version;
	
	private int modCount;
	
	private ItemReference rootNodeRef;
	
	private ItemReference versionTableRef;
	
	/**
	 * TODO: initialize and read must be transactional!
	 * @param appNode
	 * @throws IOException
	 */
	public Metadata(TransactionContext txContext) throws IOException {
		this.txContext = txContext;
	}
	
	public VersionTable getVersionTable() {
		return new VersionTable();
	}
	
	public BTreeNode getRootNode() {
		// TODO
		return null;
	}

	private DataInputStream initialize() {
	    // TODO Auto-generated method stub
		return null;
    }
}

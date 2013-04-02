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
package org.apache.people.mreutegg.jsinfonia.group;

import java.util.ArrayList;
import java.util.List;

import org.apache.people.mreutegg.jsinfonia.MemoryNode;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeTestBase;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.group.MemoryNodeGroupClient;
import org.apache.people.mreutegg.jsinfonia.group.MemoryNodeGroupMember;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.junit.Test;

public class GroupMemoryNodeIT extends MemoryNodeTestBase {

    @Test
	public void testSinfoniaGroup() throws Exception {
    	testSinfoniaGroup(1, 3, 1024, 128, 64);
    	//testSinfoniaGroup(2, 3, 1024, 128, 64);
    	//testSinfoniaGroup(4, 3, 1024, 128, 64);
    	//testSinfoniaGroup(8, 3, 1024, 128, 64);
    }
    
    private void testSinfoniaGroup(
    		final int numMemoryNodes,
    		final int numReplicas,
    		final int addressSpace,
    		final int itemSize,
    		final int numThreads) throws Exception {
    	List<MemoryNodeGroupMember> members = new ArrayList<MemoryNodeGroupMember>();
    	SimpleMemoryNodeDirectory<MemoryNodeGroupClient> directory = new SimpleMemoryNodeDirectory<MemoryNodeGroupClient>();
    	for (int i = 0; i < numMemoryNodes; i++) {
    		for (int j = 0; j < numReplicas; j++) {
        		MemoryNode memoryNode = new InMemoryMemoryNode(i, addressSpace, itemSize);
    			members.add(new MemoryNodeGroupMember(memoryNode));
    		}
    		directory.addMemoryNode(new MemoryNodeGroupClient(i));
    	}
    	
    	testSinfonia(directory, addressSpace, itemSize, numThreads);
    	
    	for (Integer memoryNodeId : directory.getMemoryNodeIds()) {
    		directory.getMemoryNode(memoryNodeId).close();
    	}
    	for (MemoryNodeGroupMember groupMember : members) {
    		groupMember.close();
    	}
    }   
}

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
package org.apache.people.mreutegg.jsinfonia.mem;

import org.apache.people.mreutegg.jsinfonia.MemoryNodeTestBase;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.mem.InMemoryMemoryNode;
import org.junit.Test;

public class InMemoryMemoryNodeTest extends MemoryNodeTestBase {

    @Test
    public void inMemory() throws Exception {
        testSinfoniaInMemory(10, 1024, 4096, 64);
        testSinfoniaInMemory(10, 1024, 1024, 64);
        testSinfoniaInMemory(10, 1024, 512, 64);
        testSinfoniaInMemory(10, 1024, 128, 64);
        testSinfoniaInMemory(10, 1024, 32, 64);
        System.out.println();
        testSinfoniaInMemory(10, 1024, 128, 128);
        testSinfoniaInMemory(10, 1024, 128, 64);
        testSinfoniaInMemory(10, 1024, 128, 32);
        testSinfoniaInMemory(10, 1024, 128, 16);
        System.out.println();
        testSinfoniaInMemory(1, 1024, 128, 64);
        testSinfoniaInMemory(2, 1024, 128, 64);
        testSinfoniaInMemory(4, 1024, 128, 64);
        testSinfoniaInMemory(8, 1024, 128, 64);
        testSinfoniaInMemory(16, 1024, 128, 64);
        testSinfoniaInMemory(32, 1024, 128, 64);
    }

    private void testSinfoniaInMemory(
            final int numMemoryNodes,
            final int addressSpace,
            final int itemSize,
            final int numThreads) throws Exception {
        SimpleMemoryNodeDirectory<InMemoryMemoryNode> directory = new SimpleMemoryNodeDirectory<InMemoryMemoryNode>();
        for (int i = 0; i < numMemoryNodes; i++) {
            directory.addMemoryNode(
                    new InMemoryMemoryNode(i, addressSpace, itemSize));
        }
        testSinfonia(directory, addressSpace, itemSize, numThreads);
    }
    
}

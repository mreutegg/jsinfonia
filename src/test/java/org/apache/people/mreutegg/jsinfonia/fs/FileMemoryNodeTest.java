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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.people.mreutegg.jsinfonia.MemoryNodeTestBase;
import org.apache.people.mreutegg.jsinfonia.SimpleMemoryNodeDirectory;
import org.apache.people.mreutegg.jsinfonia.fs.FileMemoryNode;
import org.junit.Assert;
import org.junit.Test;

public class FileMemoryNodeTest extends MemoryNodeTestBase {

    @Test
	public void testSinfoniaFile() throws Exception {
    	testSinfoniaFile(1, 16 * 1024, 1024, 1024, 256);
    }
    
    private void testSinfoniaFile(
    		final int numMemoryNodes,
    		final int addressSpace,
    		final int itemSize,
    		final int bufferSize,
    		int numThreads) throws Exception {
    	File testDir = new File(new File("target"), "memoryNodes");
    	if (testDir.exists()) {
    		delete(testDir);
    	}
        SimpleMemoryNodeDirectory<FileMemoryNode> directory = new SimpleMemoryNodeDirectory<FileMemoryNode>();
    	ExecutorService executor = Executors.newFixedThreadPool(numMemoryNodes);
    	try {
	        for (int i = 0; i < numMemoryNodes; i++) {
	        	File memoryNodeDir = new File(testDir, ""+ i);
	        	if (!memoryNodeDir.mkdirs()) {
	        		Assert.fail("Unable to create memory node directory: " + memoryNodeDir.getAbsolutePath());
	        	}
	        	FileMemoryNode fmn = new FileMemoryNode(i, new File(memoryNodeDir, "data"), 
    					addressSpace, itemSize, bufferSize);
	        	directory.addMemoryNode(fmn);
	        }
			testSinfonia(directory, addressSpace, itemSize, numThreads);
        } finally {
        	Collection<Callable<Void>> closes = new ArrayList<Callable<Void>>();
	        for (int i = 0; i < numMemoryNodes; i++) {
	        	final FileMemoryNode fmn = directory.getMemoryNode(i); 
	        	closes.add(new Callable<Void>() {
					@Override
                    public Void call() throws Exception {
						fmn.close();
	                    return null;
                    }
	        	});
	        }
        	executor.invokeAll(closes);
        	executor.shutdown();
        	executor.awaitTermination(60, TimeUnit.SECONDS);
        	directory = null;
	        delete(testDir);
        }
    }
}

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

import java.util.Random;

import org.apache.people.mreutegg.jsinfonia.data.DataItem;


import junit.framework.TestCase;

public class DataItemTest extends TestCase {

	private DataItem item;
	
	public void testDataItem() {
		item = new DataItem(new byte[1024]);
		doTest(0);
		doTest(Integer.MAX_VALUE);
		doTest(Integer.MIN_VALUE);
		Random rand = new Random();
		for (int i = 0; i < 1000 * 1000; i++) {
			doTest(rand.nextInt());
		}
	}
	
	public void doTest(int version) {
		item.setVersion(version);
		assertEquals(version, item.getVersion());
	}
}

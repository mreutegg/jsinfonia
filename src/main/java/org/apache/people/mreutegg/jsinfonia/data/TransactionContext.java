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

import org.apache.people.mreutegg.jsinfonia.ItemReference;

public interface TransactionContext {

	/**
	 * Perform a read operation within this transaction context.
	 * 
	 * @param reference
	 *            the item reference from where to read.
	 * @param op
	 *            the read operation to perform.
	 * 
	 * @return the result of the read operation.
	 */
	public <T> T read(ItemReference reference, DataOperation<T> op);

	/**
	 * Perform a write operation within this transaction context.
	 * 
	 * @param reference
	 *            the item reference where to write.
	 * @param op
	 *            the write operation to perform.
	 * 
	 * @return the result of the write operation.
	 */
	public <T> T write(ItemReference reference, DataOperation<T> op);
}

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

/**
 * Performs a series of operations on DataItems and make sure
 * the operations are performed on a consistent view of the data.
 * <p/>
 * A transaction is executed against a TransactionContext and
 * the {@link #perform(TransactionManager)} method is called for
 * every attempt to perform the transaction using the provided
 * {@link TransactionManager} to read and write data.
 */
public abstract class Transaction<T> {

	/**
	 * This method is called to actually perform the operations of
	 * this transaction. The implementation must only perform
	 * operations using the given {@link TransactionManager}. After
	 * this method returns the transaction context will try to commit
	 * the transaction and return the result. The transaction is
	 * retried if the operations performed on the transaction context cannot
	 * be committed.
	 * 
	 * @param txContext the transaction context.
	 * @return the result object.
	 */
	public abstract T perform(TransactionContext txContext);
	
}

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
package org.apache.people.mreutegg.jsinfonia.util;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.Transaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;

public class SinfoniaHashMap<K, V> extends AbstractMap<K, V> {

	private final TransactionManager txManager;
	private final ItemManagerFactory factory;
	private final ItemReference headerRef;
	private final BucketReader<Entry<K, V>> reader;
	private final BucketWriter<Entry<K, V>> writer;
	
	public SinfoniaHashMap(TransactionManager txManager, ItemManagerFactory factory,
			ItemReference headerRef, BucketReader<Entry<K, V>> reader,
			BucketWriter<Entry<K, V>> writer) {
		this.txManager = txManager;
		this.factory = factory;
		this.headerRef = headerRef;
		this.reader = reader;
		this.writer = writer;
	}
	
	
	
	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return txManager.execute(new Transaction<Set<Map.Entry<K, V>>>() {
			@Override
			public Set<java.util.Map.Entry<K, V>> perform(
					TransactionContext txContext) {
				return createMap(txContext).entrySet();
			}
		});
	}

	@Override
	public V put(final K key, final V value) {
		if (key == null) {
			throw new NullPointerException("key must not be null");
		}
		if (value == null) {
			throw new NullPointerException("value must not be null");
		}
		return txManager.execute(new Transaction<V>() {
			@Override
			public V perform(TransactionContext txContext) {
				return createMap(txContext).put(key, value);
			}
		});
	}

	@Override
	public V remove(final Object key) {
		return txManager.execute(new Transaction<V>() {
			@Override
			public V perform(TransactionContext txContext) {
				return createMap(txContext).remove(key);
			}
		});
	}

	@Override
	public V get(final Object key) {
		return txManager.execute(new Transaction<V>() {
			@Override
			public V perform(TransactionContext txContext) {
				return createMap(txContext).get(key);
			}
		});
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}



	@Override
	public void putAll(final Map<? extends K, ? extends V> m) {
		txManager.execute(new Transaction<Void>() {
			@Override
			public Void perform(TransactionContext txContext) {
				createMap(txContext).putAll(m);
				return null;
			}
		});
	}

	//-------------------------------< internal >------------------------------
	
	private Map<K, V> createMap(TransactionContext txContext) {
		SinfoniaBucketStore<K, V> store = new SinfoniaBucketStore<K, V>(
				factory.createItemManager(txContext),
				txContext, headerRef, reader, writer);
		return new LinearHashMap<K, V>(store);
	}
}

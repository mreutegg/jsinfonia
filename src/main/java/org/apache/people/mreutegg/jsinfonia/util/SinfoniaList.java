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

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.DataOperation;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 * Implementation of a linked List storing data on memory nodes.
 * <p/>
 * A SinfoniaList is initialized with a header reference with the
 * following structure:
 * <pre>
 * +---------------------------------------------------------------------------+
 * | version |         |         |        |            size (4 byte)           |
 * +---------------------------------------------------------------------------+
 * |      head ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
 * +---------------------------------------------------------------------------+
 * |      tail ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
 * +---------------------------------------------------------------------------+
 * </pre>
 * The first byte contains a version tag. At offset 4, the size of the list is
 * stored. At offset 8 and 16 the head and the tail buckets are referenced.
 * <p/>
 * Each linked bucket has the following structure:
 * <pre>
 * +---------------------------------------------------------------------------+
 * |      next ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
 * +---------------------------------------------------------------------------+
 * |                               bucket payload                              |
 * +---------------------------------------------------------------------------+
 * |                                    ...                                    |
 * +---------------------------------------------------------------------------+
 * </pre>
 * The next item reference either points to the next bucket if there are more
 * items in the list or points to the header item if it is the tail bucket in
 * the linked list. That is, the next reference is equal to {@link #headerRef}.
 * 
 * @param <E> type of elements in this list.
 */
public class SinfoniaList<E> extends AbstractList<E> {

	private static final int HEADER_OFFSET_VERSION = 0;
	private static final int HEADER_OFFSET_SIZE = 4;
	private static final int HEADER_OFFSET_HEAD = 8;
	private static final int HEADER_OFFSET_TAIL = 16;
	private static final int HEADER_OFFSET_BUCKET_PAYLOAD = 8;

	private final ItemReference headerRef;
	private final TransactionContext txContext;
	private final ItemManagerFactory factory;
	private final BucketReader<E> reader;
	private final BucketWriter<E> writer;
	
	public SinfoniaList(
			ItemReference headerRef,
			TransactionContext txContext,
			ItemManagerFactory factory,
			BucketReader<E> reader,
			BucketWriter<E> writer) {
		this.headerRef = headerRef;
		this.txContext = txContext;
		this.factory = factory;
		this.reader = reader;
		this.writer = writer;
	}
	
	public static <E> List<E> newList(
			final ItemReference headerRef,
			TransactionContext txContext,
			ItemManagerFactory factory,
			BucketReader<E> reader,
			BucketWriter<E> writer) {
		txContext.write(headerRef, new DataOperation<Void>() {
			@Override
			public Void perform(ByteBuffer data) {
				data.position(HEADER_OFFSET_SIZE);
				data.putInt(0);
				headerRef.toByteBuffer(data);
				headerRef.toByteBuffer(data);
				return null;
			}
		});
		return new SinfoniaList<E>(
				headerRef, txContext, factory, reader, writer);
	}
	
	@Override
	public E get(int index) {
		return Iterators.get(iterator(), index);
	}
	
	@Override
	public Iterator<E> iterator() {
		return new Iterator<E>() {

			Iterator<Iterator<E>> iterators = getBucketReaders();
			Iterator<E> currentIt = nextIterator();
			E nextElement = fetchNext(); 
			
			@Override
			public boolean hasNext() {
				return nextElement != null;
			}

			@Override
			public E next() {
				E next = nextElement;
				nextElement = fetchNext();
				return next;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
			private Iterator<E> nextIterator() {
				return iterators.hasNext() ? iterators.next() : null;
			}
			
			private E fetchNext() {
				if (currentIt == null) {
					return null;
				} else if (currentIt.hasNext()) {
					return currentIt.next();
				} else {
					currentIt = nextIterator();
				}
				return fetchNext();
			}
		};
	}

	@Override
	public int size() {
		return txContext.read(headerRef, new DataOperation<Integer>() {
			@Override
			public Integer perform(ByteBuffer data) {
				return data.getInt(HEADER_OFFSET_SIZE);
			}
		});
	}
	
	@Override
	public boolean add(E e) {
		if (addInternal(e)) {
			return true;
		} else {
			addBucket();
			return addInternal(e);
		}
	}
	
	//--------------------------< internal >-----------------------------------

	/**
	 * Adds the given element to the tail bucket if there is still space
	 * left in the bucket and increments the size of the list. Also returns
	 * <code>false</code> if there is no bucket yet.
	 * 
	 * @param e the element to add.
	 * @return <code>true</code> if it was added; <code>false</code> otherwise.
	 */
	private boolean addInternal(E e) {
		ItemReference bucketRef = getTailRef();
		if (bucketRef.equals(headerRef)) {
			// no bucket
			return false;
		}
		final Iterable<E> entries = Iterables.concat(txContext.read(bucketRef,
				new DataOperation<Iterable<E>>() {
					@Override
					public Iterable<E> perform(ByteBuffer data) {
						data.position(HEADER_OFFSET_BUCKET_PAYLOAD);
						return reader.read(data.slice());
					}
		}), Collections.singleton(e));
		int num = txContext.write(bucketRef, new DataOperation<Integer>() {
			@Override
			public Integer perform(ByteBuffer data) {
				data.position(HEADER_OFFSET_BUCKET_PAYLOAD);
				return writer.write(entries, data.slice());
			}
		});
		if (Iterables.size(entries) == num) {
			txContext.write(headerRef, new DataOperation<Void>() {
				@Override
				public Void perform(ByteBuffer data) {
					data.putInt(HEADER_OFFSET_SIZE, 
							data.getInt(HEADER_OFFSET_SIZE) + 1);
					return null;
				}
			});
			return true;
		}
		return false;
	}
	
	/**
	 * Adds a new tail bucket.
	 * 
	 * @return the reference to the added bucket.
	 */
	private ItemReference addBucket() {
		ItemManager mgr = factory.createItemManager(txContext);
		final ItemReference newTail = mgr.alloc();
		ItemReference oldTail = setTailRef(newTail);
		// link new tail to header
		txContext.write(newTail, new DataOperation<Void>() {
			@Override
			public Void perform(ByteBuffer data) {
				headerRef.toByteBuffer(data);
				return null;
			}
		});
		if (oldTail.equals(headerRef)) {
			setHeadRef(newTail);
		} else {
			txContext.write(oldTail, new DataOperation<Void>() {
				@Override
				public Void perform(ByteBuffer data) {
					newTail.toByteBuffer(data);
					return null;
				}
			});
		}
		return newTail;
	}
	
	private Iterator<Iterator<E>> getBucketReaders() {
		return new Iterator<Iterator<E>>() {

			ItemReference nextBucketRef = getHeadRef();
			Iterator<E> nextIt = getIterator();
			
			@Override
			public boolean hasNext() {
				return nextIt != null;
			}

			@Override
			public Iterator<E> next() {
				Iterator<E> next = nextIt;
				nextBucketRef = txContext.read(nextBucketRef,
						new DataOperation<ItemReference>() {
							@Override
							public ItemReference perform(ByteBuffer data) {
								return ItemReference.fromBuffer(data);
							}
				});
				nextIt = getIterator();
				return next;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
			private Iterator<E> getIterator() {
				if (nextBucketRef.equals(headerRef)) {
					return null;
				}
				return txContext.read(nextBucketRef, new DataOperation<Iterator<E>>() {
					@Override
					public Iterator<E> perform(ByteBuffer data) {
						data.position(HEADER_OFFSET_BUCKET_PAYLOAD);
						return reader.read(data.slice()).iterator();
					}
				});
			}
			
		};
	}

	private ItemReference getHeadRef() {
		return txContext.read(headerRef, new DataOperation<ItemReference>() {
			@Override
			public ItemReference perform(ByteBuffer data) {
				data.position(HEADER_OFFSET_HEAD);
				return ItemReference.fromBuffer(data);
			}
		});
	}

	private ItemReference setHeadRef(final ItemReference ref) {
		return txContext.write(headerRef, new DataOperation<ItemReference>() {
			@Override
			public ItemReference perform(ByteBuffer data) {
				data.position(HEADER_OFFSET_HEAD);
				ItemReference currentHead = ItemReference.fromBuffer(data);
				data.position(HEADER_OFFSET_HEAD);
				ref.toByteBuffer(data);
				return currentHead;
			}
		});
	}

	private ItemReference getTailRef() {
		return txContext.read(headerRef, new DataOperation<ItemReference>() {
			@Override
			public ItemReference perform(ByteBuffer data) {
				data.position(HEADER_OFFSET_TAIL);
				return ItemReference.fromBuffer(data);
			}
		});
	}
	
	private ItemReference setTailRef(final ItemReference ref) {
		return txContext.write(headerRef, new DataOperation<ItemReference>() {
			@Override
			public ItemReference perform(ByteBuffer data) {
				data.position(HEADER_OFFSET_TAIL);
				ItemReference currentTail = ItemReference.fromBuffer(data);
				data.position(HEADER_OFFSET_TAIL);
				ref.toByteBuffer(data);
				return currentTail;
			}
		});
	}
}

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
import java.util.List;

import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.DataOperation;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is inspired by Apache Lucene FixedBitSet but data
 * is located in memory node items.
 * 
 * The header item has the following structure:
 * <pre>
 * +---------------------------------------------------------------------------+
 * | version |         | numRefs (2 byte) |           numBits (4 byte)         |
 * +---------------------------------------------------------------------------+
 * |            numWords (4 byte)         |         numWords (4 byte)          |
 * +---------------------------------------------------------------------------+
 * |                  ....                |               ....                 |
 * +---------------------------------------------------------------------------+
 * |           ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
 * +---------------------------------------------------------------------------+
 * |           ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
 * +---------------------------------------------------------------------------+
 * |                                    .....                                  |
 * +---------------------------------------------------------------------------+
 * </pre>
 * 
 */
public class FixedBitSet {

	private static final Logger log = LoggerFactory.getLogger(FixedBitSet.class);	
	
	/**
	 * The length of the meta data (version, numRefs, numBits)
	 */
	private static final int META_LENGTH = 8;
	
	private static final int OFFSET_NUM_REFS = 2;
	private static final int OFFSET_NUM_BITS = 4;
	
	private final TransactionContext context;
	
	private final ItemReference headerRef;
	
	/**
	 * Creates a <code>FixedBitSet</code> based on existing data. The given
	 * <code>ItemReference</code> identifies the header item.
	 * 
	 * @param context the transaction context to access items.
	 * @param headerRef the reference to the header item.
	 */
	public FixedBitSet(TransactionContext context, ItemReference headerRef) {
		this.context = context;
		this.headerRef = headerRef;
	}
	
	/**
	 * Creates a <code>FixedBitSet</code> and initializes the header item
	 * with the passed <code>dataItemRefs</code> to store the actual bits.
	 * 
	 * @param context the transaction context to access items.
	 * @param headerRef the reference to the header item.
	 * @param dataItemRefs the references to the data items that store
	 * 				the actual bits. FixedBitSet supports up to
	 * 				<code>Character.MAX_VALUE</code> data item references.
	 */
	public FixedBitSet(TransactionContext context,
			ItemReference headerRef, final List<ItemReference> dataItemRefs) {
		this(context, headerRef, dataItemRefs, -1);
	}

	/**
	 * Creates a <code>FixedBitSet</code> and initializes the header item
	 * with the passed <code>dataItemRefs</code> to store the actual bits.
	 * 
	 * @param context the transaction context to access items.
	 * @param headerRef the reference to the header item.
	 * @param dataItemRefs the references to the data items that store
	 * 				the actual bits. FixedBitSet supports up to
	 * 				<code>Character.MAX_VALUE</code> data item references.
	 * @param length limits the length of this bit set to the given
	 * 				<code>length</code>.
	 */
	public FixedBitSet(TransactionContext context, ItemReference headerRef,
			final List<ItemReference> dataItemRefs, final int length) {
		this.context = context;
		this.headerRef = headerRef;
		if (dataItemRefs.size() > Character.MAX_VALUE) {
			throw new IllegalArgumentException("FixedBitSet only supports up to " +
					(int) Character.MAX_VALUE + " dataItemRefs");
		}
		// check if dataItemRefs fit into the header item
		int dataSize = context.read(headerRef, new DataOperation<Integer>() {
			@Override
			public Integer perform(ByteBuffer data) {
				return data.remaining();
			}
		});
		if (dataItemRefs.size() * 12 > dataSize - META_LENGTH) {
			throw new IllegalArgumentException("Not enough space in header item (" + 
					(dataSize - META_LENGTH) + ") to store " + dataItemRefs.size() +
					" references.");
		}
		// get the words (longs) we can use to store the bits per item
		final int numWords[] = new int[dataItemRefs.size()];
		int maxWords = 0;
		int i = 0;
		for (ItemReference r : dataItemRefs) {
			numWords[i] += context.read(r, new DataOperation<Integer>() {
				@Override
				public Integer perform(ByteBuffer data) {
					// bits are stored as longs, find out how
					// many long values fit into remaining space
					return data.remaining() / 8;
				}
			});
			maxWords += numWords[i++];
		}
		if (length > maxWords * 64) {
			throw new IllegalArgumentException("length limit > capacity: " +
					length + " > " + (maxWords * 64));
		}
		// otherwise write dataItemRefs
		context.write(headerRef, new DataOperation<Void>() {
			@Override
			public Void perform(ByteBuffer data) {
				data.putChar(OFFSET_NUM_REFS, (char) dataItemRefs.size());
				// starts/offsets
				data.position(META_LENGTH);
				int maxWords = 0;
				for (int i : numWords) {
					maxWords += i;
					data.putInt(i);
				}
				for (ItemReference r : dataItemRefs) {
					data.putInt(r.getMemoryNodeId());
					data.putInt(r.getAddress());
				}
				int numBits = maxWords * 64;
				if (length >= 0) {
					numBits = Math.min(length, numBits); 
				}
				data.putInt(OFFSET_NUM_BITS, numBits);
				return null;
			}
		});
	}
	
	public boolean get(int index) {
		if (index < 0 || index >= getNumBits()) {
			throw new IllegalArgumentException("index: " + index);
		}
		int i = index >> 6; // div 64
		// signed shift will keep a negative index and force an
		// array-index-out-of-bounds-exception, removing the need for an
		// explicit check.
		int bit = index & 0x3f; // mod 64
		long bitmask = 1L << bit;
		return (getBits(i) & bitmask) != 0;
	}
	
	public void set(int index) {
		if (index < 0 || index >= getNumBits()) {
			throw new IllegalArgumentException("index: " + index);
		}
		int wordNum = index >> 6; // div 64
		int bit = index & 0x3f; // mod 64
		long bitmask = 1L << bit;
		setBit(wordNum, bitmask);
	}
	
	public void clear(int index) {
		if (index < 0 || index >= getNumBits()) {
			throw new IllegalArgumentException("index: " + index);
		}
		int wordNum = index >> 6;
		int bit = index & 0x03f;
		long bitmask = 1L << bit;
		clearBit(wordNum, bitmask);
	}

	public int nextSetBit(int index) {
		int len = length();
		if (index < 0 || index >= len) {
			throw new IllegalArgumentException("index: " + index);
		}
		int i = index >> 6;
		final int subIndex = index & 0x3f; // index within the word
		long word = getBits(i) >> subIndex; // skip all the bits to the right of
											// index

		if (word != 0) {
			return enforceLength((i << 6) + subIndex + Long.numberOfTrailingZeros(word), len);
		}

		int numWords = getNumBits() / 64;
		while (++i < numWords) {
			word = getBits(i);
			if (word != 0) {
				return enforceLength((i << 6) + Long.numberOfTrailingZeros(word), len);
			}
		}
		return -1;
	}

	public int nextClearBit(int index) {
		int len = getNumBits();
		if (index < 0 || index >= len) {
			throw new IllegalArgumentException("index: " + index);
		}
		int i = index >> 6;
		final int subIndex = index & 0x3f;    // index within the word
		long word = ~(getBits(i) >> subIndex); // skip all the bits to the right of
											    // index

		if (word != 0) {
			return enforceLength((i << 6) + subIndex + Long.numberOfTrailingZeros(word), len);
		}

		int numWords = getNumBits() / 64;
		while (++i < numWords) {
			word = ~getBits(i);
			if (word != 0) {
				return enforceLength((i << 6) + Long.numberOfTrailingZeros(word), len);
			}
		}
		return -1;
	}
	
	public int length() {
		return getNumBits();
	}

	//----------------------------------< internal >---------------------------
	
	/**
	 * Enforces length of this bit set. If the given <code>index</code>
	 * is greater than the length, then this method returns -1. Otherwise
	 * the index is returned.
	 * 
	 * @param index the index.
	 * @param length the length to enforce.
	 * @return index or -1.
	 */
	private int enforceLength(int index, int length) {
		if (index < length) {
			return index;
		} else {
			return -1;
		}
	}
	
	private void setBit(int index, final long bitMask) {
		final BitsReference ref = getBitsReference(index);
		context.write(ref, new DataOperation<Void>() {
			@Override
			public Void perform(ByteBuffer data) {
				long bits = data.getLong(ref.subIndex * 8);
				bits |= bitMask;
				data.putLong(ref.subIndex * 8, bits);
				return null;
			}
		});
	}
	
	private void clearBit(int index, final long bitMask) {
		final BitsReference ref = getBitsReference(index);
		context.write(ref, new DataOperation<Void>() {
			@Override
			public Void perform(ByteBuffer data) {
				long bits = data.getLong(ref.subIndex * 8);
				bits &= ~bitMask;
				data.putLong(ref.subIndex * 8, bits);
				return null;
			}
		});
	}
	
	private long getBits(int index) {
		final BitsReference ref = getBitsReference(index);
		return context.read(ref, new DataOperation<Long>() {
			@Override
			public Long perform(ByteBuffer data) {
				return data.getLong(ref.subIndex * 8);
			}
		});
	}

	private BitsReference getBitsReference(final int index) {
		return context.read(headerRef, new DataOperation<BitsReference>() {
			@Override
			public BitsReference perform(ByteBuffer data) {
				int numRefs = data.getChar(OFFSET_NUM_REFS);
				data.position(META_LENGTH);
				int starts[] = new int[numRefs + 1];
				int maxWords = 0;
				for (int i = 0; i < numRefs; i++) {
					starts[i] = maxWords;
					maxWords += data.getInt();
				}
				starts[numRefs] = maxWords;
				int itemIndex = subItem(index, starts);
				int subIndex = index - starts[itemIndex];
				// log.debug("BitsReference for index " + index + ": itemIndex" + itemIndex + ", subIndex: " + subIndex);
				data.position(META_LENGTH + numRefs * 4 + itemIndex * 8);
				return new BitsReference(data.getInt(), data.getInt(), subIndex);
			}
		});
	}
	
	private int getNumBits() {
		return context.read(headerRef, new DataOperation<Integer>() {
			@Override
			public Integer perform(ByteBuffer data) {
				return data.getInt(OFFSET_NUM_BITS);
			}
		});
	}
	
	private static int subItem(int n, int[] itemStarts) {
	    // item for index:
	    int size = itemStarts.length;
	    int lo = 0; // search starts array
	    int hi = size - 1; // for first element less than n, return its index
	    while (hi >= lo) {
	      int mid = (lo + hi) >>> 1;
	      int midValue = itemStarts[mid];
	      if (n < midValue)
	        hi = mid - 1;
	      else if (n > midValue)
	        lo = mid + 1;
	      else { // found a match
	        while (mid + 1 < size && itemStarts[mid + 1] == midValue) {
	          mid++; // scan to last match
	        }
	        return mid;
	      }
	    }
	    return hi;
	}
	
	private final class BitsReference extends ItemReference {
		final int subIndex;
		
		BitsReference(int memoryNodeId, int address, int subIndex) {
			super(memoryNodeId, address);
			this.subIndex = subIndex;
		}
	}
}

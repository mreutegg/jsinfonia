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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.FailTransactionException;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a <code>BucketStore</code> storing data on memory nodes.
 *
 * <p>A <code>BucketStore</code> is initialized with an <code>ItemReference</code> pointing to a
 * header item with the following structure:
 *
 * <pre>
 * +---------------------------------------------------------------------------+
 * | version |         | numDirs (2 byte) |         split-index (4 byte)       |
 * +---------------------------------------------------------------------------+
 * |            level (4 byte)            | dirSize (2 byte) | dirSize (2 byte)|
 * +---------------------------------------------------------------------------+
 * |       ....        |       ....       |                                    |
 * +---------------------------------------------------------------------------+
 * |           ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
 * +---------------------------------------------------------------------------+
 * |           ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
 * +---------------------------------------------------------------------------+
 * |                                    .....                                  |
 * +---------------------------------------------------------------------------+
 * </pre>
 *
 * The first byte contains a version tag. At offset 2, the data buffer contains the number of {@link
 * BucketDirectory} references in this header. At offset 4, the buffer contains the current value of
 * the <code>split-index</code> for the {@link LinearHashMap}. At offset 8, the buffer contains the
 * current value for <code>level</code>. At offset 12 starts a list of length <code>numDirs</code>
 * with directory sizes. That is, the number of {@link ItemReference}s per referenced {@link
 * BucketDirectory}. Following the <code>dirSize</code> entries are the {@link ItemReference}s to
 * the {@link BucketDirectory} items.
 *
 * <p>The maximum number of <code>Bucket</code>s supported by this bucket store depends on the item
 * size. E.g. with an item size of 1024 bytes, the usable number of bytes is 1019. This allows to
 * store 100 references to bucket directories. Each {@link BucketDirectory} then contains up to 126
 * references to buckets. This means, with an item size of 1024 bytes, this bucket store will be
 * able to manage 12'600 buckets. With 4k item size: 407 * 510 = 207'570 buckets.
 *
 * @param <K>
 * @param <V>
 */
public class SinfoniaBucketStore<K, V> implements BucketStore<K, V> {

  private static final Logger log = LoggerFactory.getLogger(SinfoniaBucketStore.class);

  private static final int HEADER_OFFSET_VERSION = 0;

  private static final int HEADER_OFFSET_NUM_DIRS = 2;

  private static final int HEADER_OFFSET_SPLIT_INDEX = 4;

  private static final int HEADER_OFFSET_LEVEL = 8;

  private static final int HEADER_OFFSET_DIR_SIZE = 12;

  private final ItemManager itemMgr;

  private final TransactionContext txContext;

  /** The item reference pointing to the header of this bucket store. */
  private final ItemReference headerRef;

  private final BucketReader<Entry<K, V>> reader;
  private final BucketWriter<Entry<K, V>> writer;

  private final List<MapBucket<K, V>> buckets = new BucketList();

  public SinfoniaBucketStore(
      ItemManager itemMgr,
      TransactionContext txContext,
      ItemReference headerRef,
      BucketReader<Entry<K, V>> reader,
      BucketWriter<Entry<K, V>> writer) {
    this.itemMgr = itemMgr;
    this.txContext = txContext;
    this.headerRef = headerRef;
    this.reader = reader;
    this.writer = writer;
    if (buckets.size() == 0) {
      for (int i = 0; i < getInitialNumberOfBuckets(); i++) {
        buckets.add(createBucket());
      }
    }
    getVersion();
  }

  @Override
  public int getInitialNumberOfBuckets() {
    // TODO: make configurable?
    return 4;
  }

  @Override
  public int getSplitIndex() {
    return txContext.read(headerRef, data -> data.getInt(HEADER_OFFSET_SPLIT_INDEX));
  }

  @Override
  public int incrementAndGetSplitIndex() {
    final int splitIndex = getSplitIndex() + 1;
    txContext.write(
        headerRef,
        data -> {
          data.putInt(HEADER_OFFSET_SPLIT_INDEX, splitIndex);
          return null;
        });
    return splitIndex;
  }

  @Override
  public void resetSplitIndex() {
    txContext.write(
        headerRef,
        data -> {
          data.putInt(HEADER_OFFSET_SPLIT_INDEX, 0);
          return null;
        });
  }

  @Override
  public int getLevel() {
    return txContext.read(headerRef, data -> data.getInt(HEADER_OFFSET_LEVEL));
  }

  @Override
  public void incrementLevel() {
    final int level = getLevel() + 1;
    txContext.write(
        headerRef,
        data -> {
          data.putInt(HEADER_OFFSET_LEVEL, level);
          return null;
        });
  }

  @Override
  public int getSize() {
    int size = 0;
    for (MapBucket<K, V> b : buckets) {
      size += b.getSize();
    }
    return size;
  }

  @Override
  public List<MapBucket<K, V>> getBucketList() {
    return buckets;
  }

  @Override
  public MapBucket<K, V> createBucket() {
    return createBucketInternal();
  }

  @Override
  public MapBucket<K, V> getBucket(BucketId id) {
    if (!(id instanceof SinfoniaBucketId)) {
      throw new IllegalArgumentException("id is not a " + SinfoniaBucketId.class.getSimpleName());
    }
    return new SinfoniaBucket((SinfoniaBucketId) id);
  }

  @Override
  public void disposeBucket(BucketId id) {
    if (!(id instanceof SinfoniaBucketId)) {
      throw new IllegalArgumentException("id is not a " + SinfoniaBucketId.class.getSimpleName());
    }
    itemMgr.free((SinfoniaBucketId) id);
  }

  // ------------------------------< internal >-------------------------------

  /**
   * @return the offset from the beginning of the data buffer where the directory references start.
   */
  public int getDirectoryRefOffset() {
    return txContext.read(
        headerRef,
        data -> {
          int maxDirs = (data.remaining() - HEADER_OFFSET_DIR_SIZE) / 10; // 2 + 8
          return HEADER_OFFSET_DIR_SIZE + maxDirs * 2; //
        });
  }

  private SinfoniaBucket createBucketInternal() {
    ItemReference ref = itemMgr.alloc();
    if (ref == null) {
      throwNoMoreFreeItems();
    }
    txContext.write(
        ref,
        data -> {
          // no next item reference
          data.putInt(SinfoniaBucket.NO_NEXT_MARKER).putInt(0);
          return null;
        });
    return new SinfoniaBucket(new SinfoniaBucketId(ref));
  }

  /**
   * @return the version from the bucket store header item.
   */
  private byte getVersion() {
    return txContext.read(headerRef, data -> data.get(HEADER_OFFSET_VERSION));
  }

  private char getNumDirectories() {
    return txContext.read(headerRef, data -> data.getChar(HEADER_OFFSET_NUM_DIRS));
  }

  private BucketDirectory getOrCreateDirectory(final char index) {
    final char numDirs = getNumDirectories();
    if (index < numDirs) {
      return txContext.read(
          headerRef,
          data -> {
            data.position(getDirectoryRefOffset() + index * 8);
            ItemReference dirRef = ItemReference.fromBuffer(data);
            return new BucketDirectory(dirRef);
          });
    } else {
      BucketDirectory dir = null;
      for (char i = numDirs; i <= index; i++) {
        final char currentIdx = i;
        final ItemReference dirRef = itemMgr.alloc();
        if (dirRef == null) {
          throwNoMoreFreeItems();
        }
        dir = createBucketDirectory(dirRef);
        txContext.write(
            headerRef,
            data -> {
              int dirRefOffset = getDirectoryRefOffset();
              if (dirRefOffset + (currentIdx + 1) * 8 > data.remaining()) {
                throw new FailTransactionException(new RuntimeException("BucketStore header full"));
              }
              data.putChar(HEADER_OFFSET_NUM_DIRS, (char) (currentIdx + 1));
              data.putChar(HEADER_OFFSET_DIR_SIZE + currentIdx * 2, (char) 0);
              data.position(dirRefOffset + currentIdx * 8);
              dirRef.toByteBuffer(data);
              return null;
            });
      }
      return dir;
    }
  }

  private static void throwNoMoreFreeItems() throws FailTransactionException {
    // TODO: more meaningful exception
    throw new FailTransactionException(new RuntimeException("no more free items"));
  }

  // ---------------------------< SinfoniaBucket >----------------------------

  private class BucketList extends AbstractList<MapBucket<K, V>> {

    @Override
    public MapBucket<K, V> get(final int index) {
      if (index < 0) {
        throw new ArrayIndexOutOfBoundsException(index);
      }
      MapBucket<K, V> bucket =
          txContext.read(
              headerRef,
              data -> {
                int sumDirs = 0;
                char numDirs = data.getChar(HEADER_OFFSET_NUM_DIRS);
                for (char i = 0; i < numDirs; i++) {
                  char dirSize = data.getChar(HEADER_OFFSET_DIR_SIZE + i * 2); // dirSize: 2 bytes
                  sumDirs += dirSize;
                  if (sumDirs > index) {
                    int subIndex = index - sumDirs + dirSize;
                    return getOrCreateDirectory(i).getBucket(subIndex);
                  }
                }
                return null;
              });
      if (bucket == null) {
        throw new ArrayIndexOutOfBoundsException(index);
      } else {
        return bucket;
      }
    }

    @Override
    public int size() {
      return txContext.read(
          headerRef,
          data -> {
            int size = 0;
            char numDirs = data.getChar(HEADER_OFFSET_NUM_DIRS);
            for (int i = 0; i < numDirs; i++) {
              size += data.getChar(HEADER_OFFSET_DIR_SIZE + i * 2); // dirSize: 2 bytes
            }
            return size;
          });
    }

    @Override
    public boolean add(MapBucket<K, V> e) {
      if (e.getClass() != SinfoniaBucket.class) {
        throw new IllegalArgumentException(
            "Bucket must be of type " + SinfoniaBucket.class.getSimpleName());
      }
      final SinfoniaBucket bucket = (SinfoniaBucket) e;
      char dirIndex = (char) Math.max(0, getNumDirectories() - 1);
      for (char i = dirIndex; ; i++) {
        final BucketDirectory bucketDir = getOrCreateDirectory(i);
        final char idx = i;
        if (bucketDir.addBucketReference(bucket.id)) {
          txContext.write(
              headerRef,
              data -> {
                data.putChar(HEADER_OFFSET_DIR_SIZE + idx * 2, bucketDir.getSize());
                return null;
              });
          break;
        }
      }
      return true;
    }
  }

  /**
   * A <code>SinfoniaBucket</code> contains the actual data of the store. Buckets may be linked to
   * hold entries that overflow. A bucket has the following data layout:
   *
   * <pre>
   * +---------------------------------------------------------------------------+
   * |           ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
   * +---------------------------------------------------------------------------+
   * |                      Payload as written by BucketWriter                   |
   * +---------------------------------------------------------------------------+
   * |                                    .....                                  |
   * +---------------------------------------------------------------------------+
   * </pre>
   *
   * The initial <code>ItemReference</code> points to the next bucket that contains the overflowed
   * entries. An <code>ItemReference</code> with a memoryNodeId of <code>0xFFFFFFFF</code> indicates
   * that there is no linked bucket.
   */
  private class SinfoniaBucket implements MapBucket<K, V> {

    private static final int NO_NEXT_MARKER = 0xFFFFFFFF;

    private final SinfoniaBucketId id;

    SinfoniaBucket(SinfoniaBucketId id) {
      this.id = id;
    }

    @Override
    public BucketId getId() {
      return id;
    }

    @Override
    public V put(K key, V value) {
      Map<K, V> entries = getEntries();
      V v = entries.put(key, value);
      setEntries(entries);
      return v;
    }

    @Override
    public V remove(Object key) {
      Map<K, V> entries = getEntries();
      V v = entries.remove(key);
      setEntries(entries);
      return v;
    }

    @Override
    public Iterable<K> getKeys() {
      return getEntries().keySet();
    }

    @Override
    public V get(Object key) {
      return getEntries().get(key);
    }

    @Override
    public int getSize() {
      return getEntries().size();
    }

    @Override
    public void transferTo(Map<K, V> map) {
      if (log.isDebugEnabled()) {
        log.debug("SinfoniaBucket{" + id + "}.transferTo()");
      }
      map.putAll(getEntries());
      List<ItemReference> toFree = new ArrayList<>();
      ItemReference nextRef = id;
      for (; ; ) {
        nextRef = txContext.read(nextRef, data -> ItemReference.fromBuffer(data));
        if (nextRef.getMemoryNodeId() == NO_NEXT_MARKER) {
          break;
        } else {
          if (log.isDebugEnabled()) {
            log.debug("toFree.add(" + nextRef + ")");
          }
          toFree.add(nextRef);
        }
      }
      for (ItemReference r : toFree) {
        itemMgr.free(r);
      }
      txContext.write(
          id,
          data -> {
            data.putInt(NO_NEXT_MARKER).putInt(0);
            writer.write(Collections.<K, V>emptyMap().entrySet(), data.slice());
            return null;
          });
    }

    @Override
    public boolean isOverflowed() {
      return txContext.read(id, data -> data.getInt() != NO_NEXT_MARKER);
    }

    public String toString() {
      return getEntries().toString();
    }

    private Map<K, V> getEntries() {
      final Map<K, V> entries = new HashMap<>();
      ItemReference ref = id;
      while (ref.getMemoryNodeId() != NO_NEXT_MARKER) {
        if (log.isDebugEnabled()) {
          log.debug("SinfoniaBucket{" + id + "}.getEntries() " + ref);
        }
        ref =
            txContext.read(
                ref,
                data -> {
                  ItemReference next = ItemReference.fromBuffer(data);
                  for (Map.Entry<K, V> entry : reader.read(data.slice())) {
                    entries.put(entry.getKey(), entry.getValue());
                  }
                  return next;
                });
      }
      return entries;
    }

    private void setEntries(final Map<K, V> entries) {
      final List<Map.Entry<K, V>> entryList = new ArrayList<>(entries.entrySet());
      final int[] start = {0};
      ItemReference ref = id;
      while (ref.getMemoryNodeId() != NO_NEXT_MARKER) {
        ref =
            txContext.write(
                ref,
                data -> {
                  ItemReference next = ItemReference.fromBuffer(data);
                  start[0] +=
                      writer.write(entryList.subList(start[0], entryList.size()), data.slice());
                  if (start[0] < entryList.size()) {
                    // more items to write
                    if (next.getMemoryNodeId() == NO_NEXT_MARKER) {
                      // allocate item
                      ItemReference newNext = itemMgr.alloc();
                      if (newNext == null) {
                        throwNoMoreFreeItems();
                      }
                      txContext.write(
                          newNext,
                          d -> {
                            d.putInt(NO_NEXT_MARKER).putInt(0);
                            return null;
                          });
                      data.position(0);
                      data.putInt(newNext.getMemoryNodeId());
                      data.putInt(newNext.getAddress());
                      next = newNext;
                    }
                  }
                  return next;
                });
      }
    }
  }

  private static class SinfoniaBucketId extends ItemReference implements BucketId {

    SinfoniaBucketId(int memoryNodeId, int address) {
      super(memoryNodeId, address);
    }

    public SinfoniaBucketId(ItemReference ref) {
      this(ref.getMemoryNodeId(), ref.getAddress());
    }
  }

  /**
   * A <code>BucketDirectory</code> maintains a list of <code>ItemReference</code>s. Each pointing
   * to an item that contains the data of a <code>Bucket</code>. The data layout is as follows:
   *
   * <pre>
   * +---------------------------------------------------------------------------+
   * | version |         | numRefs (2 byte) |                 |                  |
   * +---------------------------------------------------------------------------+
   * |           ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
   * +---------------------------------------------------------------------------+
   * |           ItemReference (memoryNodeId (4 byte) + address (4 byte))        |
   * +---------------------------------------------------------------------------+
   * |                                    .....                                  |
   * +---------------------------------------------------------------------------+
   * </pre>
   */
  private class BucketDirectory {

    private static final int OFFSET_NUM_REFS = 2;

    private static final int META_LENGTH = 8;

    private static final int ITEM_REF_LENGTH = 8;

    private final ItemReference itemRef;

    BucketDirectory(ItemReference itemRef) {
      this.itemRef = itemRef;
    }

    MapBucket<K, V> getBucket(final int index) {
      MapBucket<K, V> bucket =
          txContext.read(
              itemRef,
              data -> {
                char numRefs = data.getChar(OFFSET_NUM_REFS);
                if (index < numRefs) {
                  data.position(META_LENGTH + index * 8);
                  SinfoniaBucketId id = new SinfoniaBucketId(data.getInt(), data.getInt());
                  return new SinfoniaBucket(id);
                }
                return null;
              });
      if (bucket == null) {
        throw new ArrayIndexOutOfBoundsException(index);
      } else {
        return bucket;
      }
    }

    char getSize() {
      return txContext.read(itemRef, data -> data.getChar(OFFSET_NUM_REFS));
    }

    boolean addBucketReference(final ItemReference bucketRef) {
      return txContext.write(
          itemRef,
          data -> {
            char size = data.getChar(OFFSET_NUM_REFS);
            if (size < (data.remaining() - META_LENGTH) / ITEM_REF_LENGTH) {
              data.position(META_LENGTH + size * ITEM_REF_LENGTH);
              data.putInt(bucketRef.getMemoryNodeId());
              data.putInt(bucketRef.getAddress());
              data.putChar(OFFSET_NUM_REFS, (char) (size + 1));
              return true;
            } else {
              // no more space left in this directory
              return false;
            }
          });
    }
  }

  BucketDirectory createBucketDirectory(final ItemReference itemRef) {
    return txContext.write(
        itemRef,
        data -> {
          data.put(0, (byte) 1); // version
          data.putChar(BucketDirectory.OFFSET_NUM_REFS, (char) 0);
          return new BucketDirectory(itemRef);
        });
  }
}

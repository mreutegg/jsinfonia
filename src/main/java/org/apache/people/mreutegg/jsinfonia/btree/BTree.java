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
package org.apache.people.mreutegg.jsinfonia.btree;

import java.util.Collections;
import java.util.Comparator;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerFactory;

public class BTree<K, V> {

  private final TransactionManager txManager;
  private final ItemManagerFactory factory;
  private final ItemReference headerRef;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final Comparator<? super K> comparator;
  private final int maxKeys;

  /**
   * Constructs a BTree instance with the default maximum number of keys per node.
   *
   * @param txManager the transaction manager to execute B-Tree operations within transactions
   * @param factory the factory to create item managers for node allocation and removal
   * @param headerRef the item reference pointing to the B-Tree metadata header
   */
  @SuppressWarnings("unchecked")
  public BTree(TransactionManager txManager, ItemManagerFactory factory, ItemReference headerRef) {
    this(txManager, factory, headerRef,
        (Serializer<K>) StringSerializer.INSTANCE,
        (Serializer<V>) ByteArraySerializer.INSTANCE,
        (Comparator<? super K>) Comparator.naturalOrder(),
        10); // Default max keys
  }

  /**
   * Constructs a BTree instance with a specified maximum number of keys per node.
   *
   * @param txManager the transaction manager to execute B-Tree operations within transactions
   * @param factory the factory to create item managers for node allocation and removal
   * @param headerRef the item reference pointing to the B-Tree metadata header
   * @param maxKeys the maximum number of keys a node can hold before splitting
   */
  @SuppressWarnings("unchecked")
  public BTree(
      TransactionManager txManager,
      ItemManagerFactory factory,
      ItemReference headerRef,
      int maxKeys) {
    this(txManager, factory, headerRef,
        (Serializer<K>) StringSerializer.INSTANCE,
        (Serializer<V>) ByteArraySerializer.INSTANCE,
        (Comparator<? super K>) Comparator.naturalOrder(),
        maxKeys);
  }

  /**
   * Constructs a BTree instance with custom serializers, comparator, and default max keys.
   */
  public BTree(
      TransactionManager txManager,
      ItemManagerFactory factory,
      ItemReference headerRef,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Comparator<? super K> comparator) {
    this(txManager, factory, headerRef, keySerializer, valueSerializer, comparator, 10);
  }

  /**
   * Constructs a BTree instance with custom serializers, comparator, and specified max keys.
   */
  public BTree(
      TransactionManager txManager,
      ItemManagerFactory factory,
      ItemReference headerRef,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer,
      Comparator<? super K> comparator,
      int maxKeys) {
    this.txManager = txManager;
    this.factory = factory;
    this.headerRef = headerRef;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.comparator = comparator;
    this.maxKeys = maxKeys;
  }

  /**
   * Initializes the B-Tree by allocating a root leaf node and writing the metadata header. This
   * method must be called before performing any lookup, insertion, update, or deletion.
   */
  public void initialize() {
    txManager.execute(
        txContext -> {
          ItemManager itemMgr = factory.createItemManager(txContext);
          ItemReference rootRef = itemMgr.alloc();
          LeafNode<K, V> root = new LeafNode<>(txContext, rootRef, keySerializer, valueSerializer);
          root.save();
          Metadata metadata = new Metadata(txContext, headerRef);
          metadata.initialize(rootRef);
          return null;
        });
  }

  /**
   * Looks up the value associated with the specified key in the B-Tree.
   *
   * @param key the key whose associated value is to be returned
   * @return the value associated with the key, or {@code null} if the key is not found
   */
  public V lookup(final K key) {
    return txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          BTreeNode<K, V> node = BTreeNode.load(txContext, metadata.getRootNodeRef(), keySerializer, valueSerializer);
          while (node instanceof InternalNode<K, V> internal) {
            int i = Collections.binarySearch(internal.keys, key, comparator);
            if (i < 0) {
              i = -i - 1;
            } else {
              i++;
            }
            node = BTreeNode.load(txContext, internal.getChild(i), keySerializer, valueSerializer);
          }
          LeafNode<K, V> leaf = (LeafNode<K, V>) node;
          int i = Collections.binarySearch(leaf.keys, key, comparator);
          if (i >= 0) {
            return leaf.getValue(i);
          }
          return null;
        });
  }

  /**
   * Updates the value associated with the specified key in the B-Tree if the key already exists.
   *
   * @param key the key whose associated value is to be updated
   * @param value the new value to associate with the key
   * @return {@code true} if the key was found and successfully updated, or {@code false} otherwise
   */
  public boolean update(final K key, final V value) {
    return txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          BTreeNode<K, V> node = BTreeNode.load(txContext, metadata.getRootNodeRef(), keySerializer, valueSerializer);
          while (node instanceof InternalNode<K, V> internal) {
            int i = Collections.binarySearch(internal.keys, key, comparator);
            if (i < 0) {
              i = -i - 1;
            } else {
              i++;
            }
            node = BTreeNode.load(txContext, internal.getChild(i), keySerializer, valueSerializer);
          }
          LeafNode<K, V> leaf = (LeafNode<K, V>) node;
          int i = Collections.binarySearch(leaf.keys, key, comparator);
          if (i >= 0) {
            leaf.updateValue(i, value);
            leaf.save();
            return true;
          }
          return false;
        });
  }

  /**
   * Inserts the specified key-value pair into the B-Tree. If the key already exists, its associated
   * value is updated with the new value.
   *
   * @param key the key to insert or update
   * @param value the value to associate with the key
   */
  public void insert(final K key, final V value) {
    txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          ItemReference rootRef = metadata.getRootNodeRef();
          BTreeNode<K, V> root = BTreeNode.load(txContext, rootRef, keySerializer, valueSerializer);
          if (root.getKeyCount() == maxKeys) {
            ItemManager itemMgr = factory.createItemManager(txContext);
            ItemReference newRootRef = itemMgr.alloc();
            InternalNode<K, V> newRoot = new InternalNode<>(txContext, newRootRef, keySerializer, valueSerializer);
            newRoot.addChild(0, rootRef);
            splitChild(txContext, itemMgr, newRoot, 0, root);
            metadata.setRootNodeRef(newRootRef);
            newRoot.save();
            insertNonFull(txContext, itemMgr, newRoot, key, value);
          } else {
            insertNonFull(txContext, factory.createItemManager(txContext), root, key, value);
          }
          return null;
        });
  }

  /**
   * Deletes the specified key and its associated value from the B-Tree, performing dynamic
   * underflow rebalancing on node paths where necessary.
   *
   * @param key the key to be deleted
   * @return {@code true} if the key was found and successfully deleted, or {@code false} otherwise
   */
  public boolean delete(final K key) {
    return txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          ItemReference rootRef = metadata.getRootNodeRef();
          BTreeNode<K, V> root = BTreeNode.load(txContext, rootRef, keySerializer, valueSerializer);
          ItemManager itemMgr = factory.createItemManager(txContext);
          return delete(txContext, itemMgr, root, key, metadata, rootRef);
        });
  }

  // ---------------------------------< internal >----------------------------

  private void insertNonFull(
      TransactionContext txContext, ItemManager itemMgr, BTreeNode<K, V> node, K key, V value) {
    if (node instanceof LeafNode<K, V> leaf) {
      int i = Collections.binarySearch(leaf.keys, key, comparator);
      if (i >= 0) {
        leaf.updateValue(i, value);
      } else {
        leaf.addEntry(-i - 1, key, value);
      }
      leaf.save();
    } else {
      InternalNode<K, V> internal = (InternalNode<K, V>) node;
      int i = Collections.binarySearch(internal.keys, key, comparator);
      if (i < 0) {
        i = -i - 1;
      } else {
        i++;
      }
      BTreeNode<K, V> child = BTreeNode.load(txContext, internal.getChild(i), keySerializer, valueSerializer);
      if (child.getKeyCount() == maxKeys) {
        splitChild(txContext, itemMgr, internal, i, child);
        if (comparator.compare(key, internal.getKey(i)) > 0) {
          i++;
        }
        child = BTreeNode.load(txContext, internal.getChild(i), keySerializer, valueSerializer);
      }
      insertNonFull(txContext, itemMgr, child, key, value);
    }
  }

  private void splitChild(
      TransactionContext txContext,
      ItemManager itemMgr,
      InternalNode<K, V> parent,
      int index,
      BTreeNode<K, V> child) {
    int mid = maxKeys / 2;
    K midKey = child.getKey(mid);
    ItemReference nextRef = itemMgr.alloc();
    BTreeNode<K, V> next;
    if (child instanceof LeafNode<K, V> leaf) {
      LeafNode<K, V> nextLeaf = new LeafNode<>(txContext, nextRef, keySerializer, valueSerializer);
      for (int i = mid; i < maxKeys; i++) {
        nextLeaf.addEntry(nextLeaf.getKeyCount(), leaf.getKey(mid), leaf.getValue(mid));
        leaf.removeEntry(mid);
      }
      next = nextLeaf;
    } else {
      InternalNode<K, V> internal = (InternalNode<K, V>) child;
      InternalNode<K, V> nextInternal = new InternalNode<>(txContext, nextRef, keySerializer, valueSerializer);
      nextInternal.addChild(0, internal.removeChild(mid + 1));
      for (int i = mid + 1; i < maxKeys; i++) {
        nextInternal.addKey(nextInternal.getKeyCount(), internal.removeKey(mid + 1));
        nextInternal.addChild(nextInternal.getKeyCount(), internal.removeChild(mid + 1));
      }
      internal.removeKey(mid);
      next = nextInternal;
    }
    child.save();
    next.save();
    parent.addKey(index, midKey);
    parent.addChild(index + 1, nextRef);
    parent.save();
  }

  private boolean delete(
      TransactionContext txContext,
      ItemManager itemMgr,
      BTreeNode<K, V> node,
      K key,
      Metadata metadata,
      ItemReference nodeRef) {

    int minKeys = (maxKeys - 1) / 2;
    boolean isRoot = nodeRef.equals(metadata.getRootNodeRef());

    if (node instanceof LeafNode<K, V> leaf) {
      int i = Collections.binarySearch(leaf.keys, key, comparator);
      if (i >= 0) {
        leaf.removeEntry(i);
        leaf.save();
        return true;
      }
      return false;
    }

    // node is an InternalNode
    InternalNode<K, V> internal = (InternalNode<K, V>) node;
    int idx = Collections.binarySearch(internal.keys, key, comparator);
    if (idx < 0) {
      idx = -idx - 1;
    } else {
      idx++;
    }

    ItemReference childRef = internal.getChild(idx);
    BTreeNode<K, V> child = BTreeNode.load(txContext, childRef, keySerializer, valueSerializer);

    if (child.getKeyCount() >= minKeys + 1) {
      return delete(txContext, itemMgr, child, key, metadata, childRef);
    }

    // child has only minKeys keys. We must fill or merge.
    if (idx > 0) {
      ItemReference leftRef = internal.getChild(idx - 1);
      BTreeNode<K, V> left = BTreeNode.load(txContext, leftRef, keySerializer, valueSerializer);
      if (left.getKeyCount() >= minKeys + 1) {
        borrowFromLeft(txContext, internal, idx, child, left);
        return delete(txContext, itemMgr, child, key, metadata, childRef);
      }
    }

    if (idx < internal.getKeyCount()) {
      ItemReference rightRef = internal.getChild(idx + 1);
      BTreeNode<K, V> right = BTreeNode.load(txContext, rightRef, keySerializer, valueSerializer);
      if (right.getKeyCount() >= minKeys + 1) {
        borrowFromRight(txContext, internal, idx, child, right);
        return delete(txContext, itemMgr, child, key, metadata, childRef);
      }
    }

    // Both left and right siblings have only minKeys keys. We must merge.
    if (idx > 0) {
      ItemReference leftRef = internal.getChild(idx - 1);
      BTreeNode<K, V> left = BTreeNode.load(txContext, leftRef, keySerializer, valueSerializer);
      merge(txContext, itemMgr, internal, idx - 1, left, child);

      if (isRoot && internal.getKeyCount() == 0) {
        metadata.setRootNodeRef(leftRef);
        itemMgr.free(nodeRef);
      }

      return delete(txContext, itemMgr, left, key, metadata, leftRef);
    } else {
      ItemReference rightRef = internal.getChild(idx + 1);
      BTreeNode<K, V> right = BTreeNode.load(txContext, rightRef, keySerializer, valueSerializer);
      merge(txContext, itemMgr, internal, idx, child, right);

      if (isRoot && internal.getKeyCount() == 0) {
        metadata.setRootNodeRef(childRef);
        itemMgr.free(nodeRef);
      }

      return delete(txContext, itemMgr, child, key, metadata, childRef);
    }
  }

  private void merge(
      TransactionContext txContext,
      ItemManager itemMgr,
      InternalNode<K, V> parent,
      int index,
      BTreeNode<K, V> y,
      BTreeNode<K, V> z) {

    K separatingKey = parent.removeKey(index);
    parent.removeChild(index + 1);
    parent.save();

    if (y instanceof LeafNode<K, V> yLeaf) {
      LeafNode<K, V> zLeaf = (LeafNode<K, V>) z;
      for (int i = 0; i < zLeaf.getKeyCount(); i++) {
        yLeaf.addEntry(yLeaf.getKeyCount(), zLeaf.getKey(i), zLeaf.getValue(i));
      }
      yLeaf.save();
    } else {
      InternalNode<K, V> yInternal = (InternalNode<K, V>) y;
      InternalNode<K, V> zInternal = (InternalNode<K, V>) z;
      yInternal.addKey(yInternal.getKeyCount(), separatingKey);
      yInternal.addChild(yInternal.getKeyCount(), zInternal.getChild(0));
      for (int i = 0; i < zInternal.getKeyCount(); i++) {
        yInternal.addKey(yInternal.getKeyCount(), zInternal.getKey(i));
        yInternal.addChild(yInternal.getKeyCount(), zInternal.getChild(i + 1));
      }
      yInternal.save();
    }

    itemMgr.free(z.getReference());
  }

  private void borrowFromLeft(
      TransactionContext txContext,
      InternalNode<K, V> parent,
      int index,
      BTreeNode<K, V> child,
      BTreeNode<K, V> left) {

    if (child instanceof LeafNode<K, V> childLeaf) {
      LeafNode<K, V> leftLeaf = (LeafNode<K, V>) left;

      int lastIdx = leftLeaf.getKeyCount() - 1;
      K keyToBorrow = leftLeaf.getKey(lastIdx);
      V valToBorrow = leftLeaf.getValue(lastIdx);

      leftLeaf.removeEntry(lastIdx);
      leftLeaf.save();

      childLeaf.addEntry(0, keyToBorrow, valToBorrow);
      childLeaf.save();

      parent.keys.set(index - 1, keyToBorrow);
      parent.save();
    } else {
      InternalNode<K, V> childInt = (InternalNode<K, V>) child;
      InternalNode<K, V> leftInt = (InternalNode<K, V>) left;

      K parentKey = parent.getKey(index - 1);
      int lastKeyIdx = leftInt.getKeyCount() - 1;
      K leftKey = leftInt.removeKey(lastKeyIdx);
      ItemReference leftChild = leftInt.removeChild(lastKeyIdx + 1);
      leftInt.save();

      childInt.addKey(0, parentKey);
      childInt.addChild(0, leftChild);
      childInt.save();

      parent.keys.set(index - 1, leftKey);
      parent.save();
    }
  }

  private void borrowFromRight(
      TransactionContext txContext,
      InternalNode<K, V> parent,
      int index,
      BTreeNode<K, V> child,
      BTreeNode<K, V> right) {

    if (child instanceof LeafNode<K, V> childLeaf) {
      LeafNode<K, V> rightLeaf = (LeafNode<K, V>) right;

      K keyToBorrow = rightLeaf.getKey(0);
      V valToBorrow = rightLeaf.getValue(0);

      rightLeaf.removeEntry(0);
      rightLeaf.save();

      childLeaf.addEntry(childLeaf.getKeyCount(), keyToBorrow, valToBorrow);
      childLeaf.save();

      parent.keys.set(index, rightLeaf.getKey(0));
      parent.save();
    } else {
      InternalNode<K, V> childInt = (InternalNode<K, V>) child;
      InternalNode<K, V> rightInt = (InternalNode<K, V>) right;

      K parentKey = parent.getKey(index);
      K rightKey = rightInt.removeKey(0);
      ItemReference rightChild = rightInt.removeChild(0);
      rightInt.save();

      childInt.addKey(childInt.getKeyCount(), parentKey);
      childInt.addChild(childInt.getKeyCount(), rightChild);
      childInt.save();

      parent.keys.set(index, rightKey);
      parent.save();
    }
  }
}

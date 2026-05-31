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
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerFactory;

public class BTree {

  private final TransactionManager txManager;
  private final ItemManagerFactory factory;
  private final ItemReference headerRef;
  private final int maxKeys;

  /**
   * Constructs a BTree instance with the default maximum number of keys per node.
   *
   * @param txManager the transaction manager to execute B-Tree operations within transactions
   * @param factory the factory to create item managers for node allocation and removal
   * @param headerRef the item reference pointing to the B-Tree metadata header
   */
  public BTree(TransactionManager txManager, ItemManagerFactory factory, ItemReference headerRef) {
    this(txManager, factory, headerRef, 10); // Default max keys
  }

  /**
   * Constructs a BTree instance with a specified maximum number of keys per node.
   *
   * @param txManager the transaction manager to execute B-Tree operations within transactions
   * @param factory the factory to create item managers for node allocation and removal
   * @param headerRef the item reference pointing to the B-Tree metadata header
   * @param maxKeys the maximum number of keys a node can hold before splitting
   */
  public BTree(
      TransactionManager txManager,
      ItemManagerFactory factory,
      ItemReference headerRef,
      int maxKeys) {
    this.txManager = txManager;
    this.factory = factory;
    this.headerRef = headerRef;
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
          LeafNode root = new LeafNode(txContext, rootRef);
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
   * @return the byte array value associated with the key, or {@code null} if the key is not found
   */
  public byte[] lookup(final String key) {
    return txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          BTreeNode node = BTreeNode.load(txContext, metadata.getRootNodeRef());
          while (node instanceof InternalNode internal) {
            int i = Collections.binarySearch(internal.keys, key);
            if (i < 0) {
              i = -i - 1;
            } else {
              i++;
            }
            node = BTreeNode.load(txContext, internal.getChild(i));
          }
          LeafNode leaf = (LeafNode) node;
          int i = Collections.binarySearch(leaf.keys, key);
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
   * @param value the new byte array value to associate with the key
   * @return {@code true} if the key was found and successfully updated, or {@code false} otherwise
   */
  public boolean update(final String key, final byte[] value) {
    return txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          BTreeNode node = BTreeNode.load(txContext, metadata.getRootNodeRef());
          while (node instanceof InternalNode internal) {
            int i = Collections.binarySearch(internal.keys, key);
            if (i < 0) {
              i = -i - 1;
            } else {
              i++;
            }
            node = BTreeNode.load(txContext, internal.getChild(i));
          }
          LeafNode leaf = (LeafNode) node;
          int i = Collections.binarySearch(leaf.keys, key);
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
   * @param value the byte array value to associate with the key
   */
  public void insert(final String key, final byte[] value) {
    txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          ItemReference rootRef = metadata.getRootNodeRef();
          BTreeNode root = BTreeNode.load(txContext, rootRef);
          if (root.getKeyCount() == maxKeys) {
            ItemManager itemMgr = factory.createItemManager(txContext);
            ItemReference newRootRef = itemMgr.alloc();
            InternalNode newRoot = new InternalNode(txContext, newRootRef);
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
  public boolean delete(final String key) {
    return txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          ItemReference rootRef = metadata.getRootNodeRef();
          BTreeNode root = BTreeNode.load(txContext, rootRef);
          ItemManager itemMgr = factory.createItemManager(txContext);
          return delete(txContext, itemMgr, root, key, metadata, rootRef);
        });
  }

  // ---------------------------------< internal >----------------------------

  private void insertNonFull(
      TransactionContext txContext, ItemManager itemMgr, BTreeNode node, String key, byte[] value) {
    if (node instanceof LeafNode leaf) {
      int i = Collections.binarySearch(leaf.keys, key);
      if (i >= 0) {
        leaf.updateValue(i, value);
      } else {
        leaf.addEntry(-i - 1, key, value);
      }
      leaf.save();
    } else {
      InternalNode internal = (InternalNode) node;
      int i = Collections.binarySearch(internal.keys, key);
      if (i < 0) {
        i = -i - 1;
      } else {
        i++;
      }
      BTreeNode child = BTreeNode.load(txContext, internal.getChild(i));
      if (child.getKeyCount() == maxKeys) {
        splitChild(txContext, itemMgr, internal, i, child);
        if (key.compareTo(internal.getKey(i)) > 0) {
          i++;
        }
        child = BTreeNode.load(txContext, internal.getChild(i));
      }
      insertNonFull(txContext, itemMgr, child, key, value);
    }
  }

  private void splitChild(
      TransactionContext txContext,
      ItemManager itemMgr,
      InternalNode parent,
      int index,
      BTreeNode child) {
    int mid = maxKeys / 2;
    String midKey = child.getKey(mid);
    ItemReference nextRef = itemMgr.alloc();
    BTreeNode next;
    if (child instanceof LeafNode leaf) {
      LeafNode nextLeaf = new LeafNode(txContext, nextRef);
      for (int i = mid; i < maxKeys; i++) {
        nextLeaf.addEntry(nextLeaf.getKeyCount(), leaf.getKey(mid), leaf.getValue(mid));
        leaf.removeEntry(mid);
      }
      next = nextLeaf;
    } else {
      InternalNode internal = (InternalNode) child;
      InternalNode nextInternal = new InternalNode(txContext, nextRef);
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
      BTreeNode node,
      String key,
      Metadata metadata,
      ItemReference nodeRef) {

    int minKeys = (maxKeys - 1) / 2;
    boolean isRoot = nodeRef.equals(metadata.getRootNodeRef());

    if (node instanceof LeafNode leaf) {
      int i = Collections.binarySearch(leaf.keys, key);
      if (i >= 0) {
        leaf.removeEntry(i);
        leaf.save();
        return true;
      }
      return false;
    }

    // node is an InternalNode
    InternalNode internal = (InternalNode) node;
    int idx = Collections.binarySearch(internal.keys, key);
    if (idx < 0) {
      idx = -idx - 1;
    } else {
      idx++;
    }

    ItemReference childRef = internal.getChild(idx);
    BTreeNode child = BTreeNode.load(txContext, childRef);

    if (child.getKeyCount() >= minKeys + 1) {
      return delete(txContext, itemMgr, child, key, metadata, childRef);
    }

    // child has only minKeys keys. We must fill or merge.
    if (idx > 0) {
      ItemReference leftRef = internal.getChild(idx - 1);
      BTreeNode left = BTreeNode.load(txContext, leftRef);
      if (left.getKeyCount() >= minKeys + 1) {
        borrowFromLeft(txContext, internal, idx, child, left);
        return delete(txContext, itemMgr, child, key, metadata, childRef);
      }
    }

    if (idx < internal.getKeyCount()) {
      ItemReference rightRef = internal.getChild(idx + 1);
      BTreeNode right = BTreeNode.load(txContext, rightRef);
      if (right.getKeyCount() >= minKeys + 1) {
        borrowFromRight(txContext, internal, idx, child, right);
        return delete(txContext, itemMgr, child, key, metadata, childRef);
      }
    }

    // Both left and right siblings have only minKeys keys. We must merge.
    if (idx > 0) {
      ItemReference leftRef = internal.getChild(idx - 1);
      BTreeNode left = BTreeNode.load(txContext, leftRef);
      merge(txContext, itemMgr, internal, idx - 1, left, child);

      if (isRoot && internal.getKeyCount() == 0) {
        metadata.setRootNodeRef(leftRef);
        itemMgr.free(nodeRef);
      }

      return delete(txContext, itemMgr, left, key, metadata, leftRef);
    } else {
      ItemReference rightRef = internal.getChild(idx + 1);
      BTreeNode right = BTreeNode.load(txContext, rightRef);
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
      InternalNode parent,
      int index,
      BTreeNode y,
      BTreeNode z) {

    String separatingKey = parent.removeKey(index);
    parent.removeChild(index + 1);
    parent.save();

    if (y instanceof LeafNode) {
      LeafNode yLeaf = (LeafNode) y;
      LeafNode zLeaf = (LeafNode) z;
      for (int i = 0; i < zLeaf.getKeyCount(); i++) {
        yLeaf.addEntry(yLeaf.getKeyCount(), zLeaf.getKey(i), zLeaf.getValue(i));
      }
      yLeaf.save();
    } else {
      InternalNode yInternal = (InternalNode) y;
      InternalNode zInternal = (InternalNode) z;
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
      InternalNode parent,
      int index,
      BTreeNode child,
      BTreeNode left) {

    if (child instanceof LeafNode) {
      LeafNode childLeaf = (LeafNode) child;
      LeafNode leftLeaf = (LeafNode) left;

      int lastIdx = leftLeaf.getKeyCount() - 1;
      String keyToBorrow = leftLeaf.getKey(lastIdx);
      byte[] valToBorrow = leftLeaf.getValue(lastIdx);

      leftLeaf.removeEntry(lastIdx);
      leftLeaf.save();

      childLeaf.addEntry(0, keyToBorrow, valToBorrow);
      childLeaf.save();

      parent.keys.set(index - 1, keyToBorrow);
      parent.save();
    } else {
      InternalNode childInt = (InternalNode) child;
      InternalNode leftInt = (InternalNode) left;

      String parentKey = parent.getKey(index - 1);
      int lastKeyIdx = leftInt.getKeyCount() - 1;
      String leftKey = leftInt.removeKey(lastKeyIdx);
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
      InternalNode parent,
      int index,
      BTreeNode child,
      BTreeNode right) {

    if (child instanceof LeafNode) {
      LeafNode childLeaf = (LeafNode) child;
      LeafNode rightLeaf = (LeafNode) right;

      String keyToBorrow = rightLeaf.getKey(0);
      byte[] valToBorrow = rightLeaf.getValue(0);

      rightLeaf.removeEntry(0);
      rightLeaf.save();

      childLeaf.addEntry(childLeaf.getKeyCount(), keyToBorrow, valToBorrow);
      childLeaf.save();

      parent.keys.set(index, rightLeaf.getKey(0));
      parent.save();
    } else {
      InternalNode childInt = (InternalNode) child;
      InternalNode rightInt = (InternalNode) right;

      String parentKey = parent.getKey(index);
      String rightKey = rightInt.removeKey(0);
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

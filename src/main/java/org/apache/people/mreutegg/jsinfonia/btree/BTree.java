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

  public BTree(TransactionManager txManager, ItemManagerFactory factory, ItemReference headerRef) {
    this(txManager, factory, headerRef, 10); // Default max keys
  }

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

  public byte[] lookup(final String key) {
    return txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          BTreeNode node = BTreeNode.load(txContext, metadata.getRootNodeRef());
          while (node instanceof InternalNode) {
            InternalNode internal = (InternalNode) node;
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

  public boolean update(final String key, final byte[] value) {
    return txManager.execute(
        txContext -> {
          Metadata metadata = new Metadata(txContext, headerRef);
          BTreeNode node = BTreeNode.load(txContext, metadata.getRootNodeRef());
          while (node instanceof InternalNode) {
            InternalNode internal = (InternalNode) node;
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

  private void insertNonFull(
      TransactionContext txContext, ItemManager itemMgr, BTreeNode node, String key, byte[] value) {
    if (node instanceof LeafNode) {
      LeafNode leaf = (LeafNode) node;
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
    if (child instanceof LeafNode) {
      LeafNode leaf = (LeafNode) child;
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

  public boolean delete(final String key) {
    return txManager.execute(
        txContext -> {
          // Simplified delete: just find and remove from leaf.
          // In a real B-Tree, we should rebalance.
          Metadata metadata = new Metadata(txContext, headerRef);
          BTreeNode node = BTreeNode.load(txContext, metadata.getRootNodeRef());
          while (node instanceof InternalNode) {
            InternalNode internal = (InternalNode) node;
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
            leaf.removeEntry(i);
            leaf.save();
            return true;
          }
          return false;
        });
  }

  // ---------------------------------< internal >----------------------------

}

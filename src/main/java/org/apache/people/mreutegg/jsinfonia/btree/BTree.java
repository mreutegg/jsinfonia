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

import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.data.Transaction;
import org.apache.people.mreutegg.jsinfonia.data.TransactionContext;
import org.apache.people.mreutegg.jsinfonia.data.TransactionManager;
import org.apache.people.mreutegg.jsinfonia.util.ItemManagerFactory;

public class BTree {

    private final TransactionManager txManager;
    private final ItemManagerFactory factory;
    private final ItemReference headerRef;
    private final BTreeNode root;

    public BTree(TransactionManager txManager, ItemManagerFactory factory,
            ItemReference headerRef) {
        this.txManager = txManager;
        this.factory = factory;
        this.headerRef = headerRef;
        this.root = new BTreeNode();
    }

    public byte[] lookup(String key) {
        return txManager.execute(new Transaction<byte[]>() {
            @Override
            public byte[] perform(TransactionContext txContext) {
                // TODO
                return null;
            }
        });
    }

    public boolean update(String key, byte[] value) {
        return txManager.execute(new Transaction<Boolean>() {
            @Override
            public Boolean perform(TransactionContext txContext) {
                // TODO
                return false;
            }
        });
    }

    public void insert(String key, byte[] value) {
        txManager.execute(new Transaction<Void>() {
            @Override
            public Void perform(TransactionContext txContext) {
                // TODO
                return null;
            }
        });
    }

    public boolean delete(String key) {
        return txManager.execute(new Transaction<Boolean>() {
            @Override
            public Boolean perform(TransactionContext txContext) {
                // TODO
                return false;
            }
        });
    }

    public String getNext(String key) {
        return txManager.execute(new Transaction<String>() {
            @Override
            public String perform(TransactionContext txContext) {
                // TODO
                return null;
            }
        });
    }

    public String getPrevious(String key) {
        return txManager.execute(new Transaction<String>() {
            @Override
            public String perform(TransactionContext txContext) {
                // TODO
                return null;
            }
        });
    }

    //---------------------------------< internal >----------------------------

}

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
package org.apache.people.mreutegg.jsinfonia.net;

import java.util.ArrayList;
import java.util.List;

import org.apache.people.mreutegg.jsinfonia.thrift.ApplicationNodeService;
import org.apache.people.mreutegg.jsinfonia.thrift.TItem;
import org.apache.people.mreutegg.jsinfonia.thrift.TItemReference;
import org.apache.people.mreutegg.jsinfonia.thrift.TMemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.thrift.TMiniTransaction;
import org.apache.people.mreutegg.jsinfonia.thrift.TResponse;
import org.apache.people.mreutegg.jsinfonia.ApplicationNode;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.MemoryNodeInfo;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.thrift.TException;

public class ApplicationNodeServiceImpl implements ApplicationNodeService.Iface {

    private final ApplicationNode appNode;

    public ApplicationNodeServiceImpl(ApplicationNode appNode) {
        this.appNode = appNode;
    }

    @Override
    public List<TMemoryNodeInfo> getMemoryNodeInfos() throws TException {
        List<TMemoryNodeInfo> infos = new ArrayList<>();
        for (MemoryNodeInfo mni : appNode.getMemoryNodeInfos().values()) {
            TMemoryNodeInfo info = new TMemoryNodeInfo();
            info.setId(mni.getId());
            info.setAddressSpace(mni.getAddressSpace());
            info.setItemSize(mni.getItemSize());
            infos.add(info);
        }
        return infos;
    }

    @Override
    public TResponse executeTransaction(TMiniTransaction tx) throws TException {
        MiniTransaction miniTx = Utils.convert(tx);
        TResponse response = Utils.convert(appNode.executeTransaction(miniTx));
        if (response.isSuccess()) {
            List<Item> readItems = miniTx.getReadItems();
            for (int i = 0; i < readItems.size(); i++) {
                Item readItem = readItems.get(i);
                TItem item = new TItem();
                TItemReference ref = new TItemReference();
                ref.setMemoryNodeId(readItem.getMemoryNodeId());
                ref.setAddress(readItem.getAddress());
                ref.setOffset(readItem.getOffset());
                item.setReference(ref);
                item.setData(readItem.getData());
                response.addToReadItems(item);
            }
        }
        return response;
    }

}

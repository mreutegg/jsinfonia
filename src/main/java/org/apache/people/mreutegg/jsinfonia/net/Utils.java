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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.people.mreutegg.jsinfonia.thrift.TItem;
import org.apache.people.mreutegg.jsinfonia.thrift.TItemReference;
import org.apache.people.mreutegg.jsinfonia.thrift.TMiniTransaction;
import org.apache.people.mreutegg.jsinfonia.thrift.TResponse;
import org.apache.people.mreutegg.jsinfonia.thrift.TResult;
import org.apache.people.mreutegg.jsinfonia.thrift.TVote;
import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.ItemReference;
import org.apache.people.mreutegg.jsinfonia.MiniTransaction;
import org.apache.people.mreutegg.jsinfonia.Response;
import org.apache.people.mreutegg.jsinfonia.Result;
import org.apache.people.mreutegg.jsinfonia.Vote;

public class Utils {

    public static TMiniTransaction convert(MiniTransaction tx) {
        TMiniTransaction mt = new TMiniTransaction();
        mt.setTxId(tx.getTxId());
        for (Item item : tx.getCompareItems()) {
            TItemReference ref = new TItemReference();
            ref.setMemoryNodeId(item.getMemoryNodeId());
            ref.setAddress(item.getAddress());
            ref.setOffset(item.getOffset());
            mt.addToCompareItems(new TItem(ref, item.getData()));
        }
        for (Item item : tx.getReadItems()) {
            TItemReference ref = new TItemReference();
            ref.setMemoryNodeId(item.getMemoryNodeId());
            ref.setAddress(item.getAddress());
            ref.setOffset(item.getOffset());
            ref.setSize(item.getData().remaining());
            mt.addToReadItems(ref);
        }
        for (Item item : tx.getWriteItems()) {
            TItemReference ref = new TItemReference();
            ref.setMemoryNodeId(item.getMemoryNodeId());
            ref.setAddress(item.getAddress());
            ref.setOffset(item.getOffset());
            mt.addToWriteItems(new TItem(ref, item.getData()));
        }
        return mt;
    }

    public static MiniTransaction convert(TMiniTransaction tx) {
        MiniTransaction mt = new MiniTransaction(tx.getTxId());
        if (tx.isSetCompareItems()) {
            for (TItem item : tx.getCompareItems()) {
                TItemReference ref = item.getReference();
                // TODO: pass ByteBuffer as is to Item
                mt.addCompareItem(new Item(ref.getMemoryNodeId(),
                        ref.getAddress(), ref.getOffset(), item.getData()));
            }
        }
        if (tx.isSetReadItems()) {
            for (TItemReference ref : tx.getReadItems()) {
                byte[] data = new byte[ref.getSize()];
                mt.addReadItem(new Item(ref.getMemoryNodeId(),
                        ref.getAddress(), ref.getOffset(), data));
            }
        }
        if (tx.isSetWriteItems()) {
            for (TItem item : tx.getWriteItems()) {
                TItemReference ref = item.getReference();
                // TODO: pass ByteBuffer as is to Item
                mt.addWriteItem(new Item(ref.getMemoryNodeId(),
                        ref.getAddress(), ref.getOffset(), item.getData()));
            }
        }
        return mt;
    }

    public static Result convert(TResult result) {
        Vote vote = Vote.values()[result.getVote().ordinal()];
        Result r;
        switch (vote) {
            case OK:
                r = Result.OK;
                break;
            case BAD_CMP:
                Set<ItemReference> refs = new HashSet<ItemReference>();
                if (result.isSetFailedCompares()) {
                    for (TItemReference failedRef : result.getFailedCompares()) {
                        refs.add(new ItemReference(
                                failedRef.getMemoryNodeId(), failedRef.getAddress()));
                    }
                }
                r = new Result(refs);
                break;
            case BAD_FORCED:
                r = Result.BAD_FORCED;
                break;
            case BAD_IO:
                r = Result.BAD_IO;
                break;
            case BAD_LOCK:
                r = Result.BAD_LOCK;
                break;
            default:
                throw new IllegalStateException("unknown vote: " + vote);
        }
        return r;
    }

    public static TResult convert(Result result) {
        TVote v = TVote.findByValue(result.getVote().ordinal());
        TResult r = new TResult();
        r.setVote(v);
        if (result.getVote() == Vote.BAD_CMP) {
            for (ItemReference ref : result.getFailedCompares()) {
                TItemReference failedRef = new TItemReference();
                failedRef.setMemoryNodeId(ref.getMemoryNodeId());
                failedRef.setAddress(ref.getAddress());
                r.addToFailedCompares(failedRef);
            }
        }
        return r;
    }

    public static TResponse convert(Response response) {
        TResponse r = new TResponse();
        r.setSuccess(response.isSuccess());
        if (!response.isSuccess()) {
            for (ItemReference ref : response.getFailedCompares()) {
                TItemReference failedRef = new TItemReference();
                failedRef.setMemoryNodeId(ref.getMemoryNodeId());
                failedRef.setAddress(ref.getAddress());
                r.addToFailedCompares(failedRef);
            }
        }
        return r;
    }

    public static Response convert(TResponse response) {
        if (response.isSuccess()) {
            return Response.SUCCESS;
        } else {
            List<ItemReference> failedCompares = new ArrayList<ItemReference>();
            if (response.isSetFailedCompares()) {
                for (TItemReference ref : response.getFailedCompares()) {
                    failedCompares.add(new ItemReference(
                            ref.getMemoryNodeId(), ref.getAddress()));
                }
            }
            return Response.failure(failedCompares);
        }
    }

    public static ItemReference convert(TItemReference reference) {
        return new ItemReference(reference.getMemoryNodeId(),
                reference.getAddress());
    }
}

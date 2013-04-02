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
namespace java org.apache.people.mreutegg.jsinfonia.thrift

enum TVote {
	OK,
	BAD_LOCK, 
	BAD_FORCED,
	BAD_CMP,
	BAD_IO
}

struct TItemReference {
	1: i32 memoryNodeId,
	2: i32 address,
	3: i32 offset,
	4: optional i32 size
}

struct TItem {
	1: TItemReference reference,
	2: binary data
}

struct TMiniTransaction {
	1: string txId,
	2: list<TItem> compareItems,
	3: list<TItemReference> readItems,
	4: list<TItem> writeItems
}

struct TResult {
	1: TVote vote,
	2: list<TItem> readItems,
	3: list<TItemReference> failedCompares
}

struct TResponse {
	1: bool success,
	2: list<TItem> readItems,
	3: list<TItemReference> failedCompares
}

struct TMemoryNodeInfo {
	1: i32 id,
	2: i32 addressSpace,
	3: i32 itemSize
}

service MemoryNodeService {

	TMemoryNodeInfo getInfo(),
	
	TResult executeAndPrepare(1:TMiniTransaction tx, 2:set<i32> memoryNodeIds),
	
	// oneway?
	void commit(1:string txId, 2:bool commit)	
}

service ApplicationNodeService {

	list<TMemoryNodeInfo> getMemoryNodeInfos(),
	
	TResponse executeTransaction(1:TMiniTransaction tx)
}
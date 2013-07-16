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

import org.apache.thrift.TProcessor;

import org.apache.people.mreutegg.jsinfonia.thrift.MemoryNodeService;
import org.apache.people.mreutegg.jsinfonia.MemoryNode;

public class MemoryNodeServer extends ThriftServer {

    private final MemoryNodeServiceImpl impl;

    public MemoryNodeServer(MemoryNode memoryNode,
            int port, boolean nonBlocking) {
        super(port, nonBlocking);
        this.impl = new MemoryNodeServiceImpl(memoryNode);
    }

    @Override
    protected TProcessor createProcessor() {
        return new MemoryNodeService.Processor<MemoryNodeService.Iface>(impl);
    }


}

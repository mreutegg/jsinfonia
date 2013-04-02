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
package org.apache.people.mreutegg.jsinfonia;

import java.util.Map;

public interface ApplicationNode {

	/**
	 * Creates a new and empty {@link MiniTransaction}.
	 * 
	 * @return the <code>MiniTransaction</code>.
	 */
	public MiniTransaction createMiniTransaction();

	/**
	 * Returns information about the currently available memory nodes.
	 * The keys of the map identify the memory node info by their id.
	 * 
	 * @return the memory node infos.
	 */
	public Map<Integer, MemoryNodeInfo> getMemoryNodeInfos();
	
	/**
	 * Executes the given <code>MiniTransaction</code> and returns the
	 * response.
	 * 
	 * @param tx the <code>MiniTransaction</code> to execute.
	 * @return the response.
	 */
	public Response executeTransaction(MiniTransaction tx);

}
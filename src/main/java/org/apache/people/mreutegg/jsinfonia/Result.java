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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Result {

	public static final Result OK = new Result(Vote.OK);
	
	public static final Result BAD_IO = new Result(Vote.BAD_IO);
	
	public static final Result BAD_FORCED = new Result(Vote.BAD_FORCED);
	
	public static final Result BAD_LOCK = new Result(Vote.BAD_LOCK);
	
	private final Set<ItemReference> failedCompares = new HashSet<ItemReference>();
	
	private final Vote vote;
	
	private Result(Vote vote) {
		this(vote, Collections.<ItemReference>emptyList());
	}
	
	private Result(Vote vote, Iterable<ItemReference> failedCompares) {
		this.vote = vote;
		for (ItemReference ref : failedCompares) {
			this.failedCompares.add(ref);
		}
	}
	
	public Result(Iterable<ItemReference> failedCompares) {
		this(Vote.BAD_CMP, failedCompares);
	}
	
	public Vote getVote() {
		return vote;
	}
	
	public Iterable<ItemReference> getFailedCompares() {
		return failedCompares;
	}
}

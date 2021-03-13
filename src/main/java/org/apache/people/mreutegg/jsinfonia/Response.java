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

public class Response {

    public static final Response SUCCESS = new Response(true, Collections.<ItemReference>emptyList());

    private final boolean isSuccess;

    private final Set<ItemReference> failedCompares = new HashSet<>();

    private Response(boolean success, Iterable<ItemReference> failedCompares) {
        this.isSuccess = success;
        for (ItemReference ref : failedCompares) {
            this.failedCompares.add(ref);
        }
    }

    public static Response failure(Iterable<ItemReference> failedCompares) {
        return new Response(false, failedCompares);
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public Iterable<ItemReference> getFailedCompares() {
        return failedCompares;
    }
}

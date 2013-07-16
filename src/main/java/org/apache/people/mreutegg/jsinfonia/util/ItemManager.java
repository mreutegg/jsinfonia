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
package org.apache.people.mreutegg.jsinfonia.util;

import org.apache.people.mreutegg.jsinfonia.ItemReference;

public interface ItemManager {

    /**
     * Allocates an item and returns the reference to that item. This method
     * returns <code>null</code> if there are no more free items.
     *
     * @return reference to the allocated item or <code>null</code> if none is
     *         free anymore.
     */
    public ItemReference alloc();

    /**
     * Frees the item with the given reference and makes it available again
     * to others.
     *
     * @param ref reference to the item to free.
     */
    public void free(ItemReference ref);
}
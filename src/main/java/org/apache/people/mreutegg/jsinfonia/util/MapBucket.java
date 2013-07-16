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

import java.util.Map;

interface MapBucket<K, V> {

    /**
     * Returns the value for the given <code>key</code> or <code>null</code> if
     * it doesn't exist in this bucket.
     *
     * @param key
     *            the key.
     * @return the associated value or <code>null</code> if it doesn't exist.
     */
    public V get(Object key);

    /**
     * Returns all the keys in this bucket.
     * @return all the keys in this bucket.
     */
    public Iterable<K> getKeys();

    /**
     * @return the number of entries in this bucket.
     */
    public int getSize();

    /**
     * Puts the given <code>key</code>/<code>value</code> pair into this bucket.
     *
     * @param key
     *            the key.
     * @param value
     *            the value.
     * @return the value previously mapped to the given <code>key</code> or
     *         <code>null</code> if no such key was present in this bucket.
     */
    public V put(K key, V value);

    /**
     * Removes the value for the given key and returns the associated value.
     * This method returns <code>null</code> if there is no value for the given
     * <code>key</code>.
     *
     * @param key
     *            the key.
     * @return the associated value or <code>null</code>.
     */
    public V remove(Object key);

    /**
     * @return <code>true</code> if this bucket is overflowed;
     *         <code>false</code> otherwise.
     */
    public boolean isOverflowed();

    /**
     * @return the id of this bucket.
     */
    public BucketId getId();

    /**
     * Transfers all entries of this bucket into the given <code>map</code>. The
     * entries in this bucket are cleared. That is, when this method returns,
     * {@link #getSize()} will return zero.
     *
     * @param map
     *            the map where to transfer the entries to.
     */
    public void transferTo(Map<K, V> map);
}

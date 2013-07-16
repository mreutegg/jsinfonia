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

import java.nio.ByteBuffer;

public interface BucketWriter<E> {

    /**
     * Writes the entries into the bucket data buffer. This method
     * may not be able to write all <code>entries</code> into the data buffer,
     * based on the remaining space available in the buffer. The value returned
     * by this method is the number of entries actually written.
     *
     * @param entries the entries to write.
     * @param data the destination buffer.
     * @return the number of entries written.
     */
    public int write(Iterable<E> entries, ByteBuffer data);
}

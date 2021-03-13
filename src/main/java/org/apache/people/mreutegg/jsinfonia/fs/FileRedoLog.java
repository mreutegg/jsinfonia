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
package org.apache.people.mreutegg.jsinfonia.fs;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.RedoLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;


public class FileRedoLog implements RedoLog, Closeable {

    private static final Logger log = LoggerFactory.getLogger(FileRedoLog.class);

    private static final int RECORD_TYPE_WRITE = 0;

    private static final int RECORD_TYPE_COMMIT = 1;

    private static final int RECORD_TYPE_ABORT = 2;

    private final FileMemoryNode memoryNode;

    private final String path;

    private final RandomAccessFile file;

    private final RandomAccessFile readFile;

    private final FileChannel readChannel;

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final BlockingQueue<SettableFuture<Boolean>> syncQueue
                        = new LinkedBlockingQueue<>();

    private final Thread syncThread;

    private long syncCount = 0;

    // transactions in log. maps transactionID to offset in file
    private final Map<String, TxInfo> loggedTransactions = Collections.synchronizedMap(new HashMap<String, TxInfo>());

    public FileRedoLog(FileMemoryNode memoryNode, File file) throws IOException {
        if (!file.exists() && !file.createNewFile()) {
            throw new IOException("Unable to create file: " + file.getAbsolutePath());
        }
        this.path = file.getAbsolutePath();
        this.memoryNode = memoryNode;
        this.file = new RandomAccessFile(file, "rw");
        this.readFile = new RandomAccessFile(file, "r");
        this.readChannel = readFile.getChannel();
        runRecovery();
        this.syncThread = new Thread(new Runnable() {
            @Override
            public void run() {
                List<SettableFuture<Boolean>> futures = Lists.newArrayList();
                while (!shutdown.get()) {
                    futures.clear();
                    try {
                        syncQueue.drainTo(futures);
                        if (futures.isEmpty()) {
                            SettableFuture<Boolean> f = syncQueue.poll(100, TimeUnit.MILLISECONDS);
                            if (f != null) {
                                futures.add(f);
                            }
                        }
                        if (!futures.isEmpty()) {
                            try {
                                syncCount++;
                                FileRedoLog.this.file.getChannel().force(false);
                                for (SettableFuture<Boolean> f : futures) {
                                    f.set(true);
                                }
                            } catch (IOException e) {
                                for (SettableFuture<Boolean> f : futures) {
                                    f.setException(e);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
        this.syncThread.start();
    }

    @Override
    public void append(String txId, List<Item> writeItems,
            Set<Integer> memoryNodeIds) throws IOException {
        int writeSize = 0;
        List<ByteBuffer> buffers = new LinkedList<>();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);
        dout.writeInt(RECORD_TYPE_WRITE);
        dout.writeUTF(txId);
        dout.writeInt(memoryNodeIds.size());
        for (Integer memoryNodeId : memoryNodeIds) {
            dout.writeInt(memoryNodeId);
        }
        dout.writeInt(writeItems.size());
        dout.flush();
        buffers.add(ByteBuffer.wrap(out.toByteArray()));
        writeSize += out.size();

        TxInfo txInfo = new TxInfo();
        for (Item item : writeItems) {
            ByteBuffer data = item.getData();
            ByteBuffer meta = ByteBuffer.allocate(12);
            meta.putInt(item.getAddress());
            meta.putInt(item.getOffset());
            meta.putInt(data.remaining());
            meta.rewind();
            writeSize += meta.remaining();
            buffers.add(meta);
            WriteItem wi = new WriteItem(item.getAddress(),
                    item.getOffset(), writeSize, data.remaining());
            buffers.add(data);
            writeSize += data.remaining();
            txInfo.writeItems.add(wi);
        }
        synchronized (file) {
            long position = file.getFilePointer();
            txInfo.position = position;
            try {
                while (!buffers.isEmpty()) {
                    file.getChannel().write(buffers.toArray(new ByteBuffer[buffers.size()]));
                    Iterator<ByteBuffer> it = buffers.iterator();
                    while (it.hasNext()) {
                        ByteBuffer bb = it.next();
                        if (!bb.hasRemaining()) {
                            it.remove();
                        }
                    }
                }
            } catch (IOException e) {
                try {
                    // try to truncate
                    file.setLength(position);
                } catch (IOException ex) {
                    log.warn("Unable to truncate redo log", ex);
                }
                throw e;
            }
        }
        loggedTransactions.put(txId, txInfo);
        SettableFuture<Boolean> f = SettableFuture.create();
        syncQueue.add(f);
        try {
            for (;;) {
                try {
                    f.get();
                    break;
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        } catch (ExecutionException e) {
            log.warn("Unable to sync redo log", e.getCause());
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e.getCause());
            }
        }
    }

    @Override
    public Set<String> getTransactionIDs() {
        Set<String> txIds;
        synchronized (loggedTransactions) {
            txIds = new HashSet<>(loggedTransactions.keySet());
        }
        return txIds;
    }

    @Override
    public void decided(String txId, boolean commit) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(out);
            if (commit) {
                dout.writeInt(RECORD_TYPE_COMMIT);
            } else {
                dout.writeInt(RECORD_TYPE_ABORT);
            }
            dout.writeUTF(txId);
            dout.flush();
            synchronized (file) {
                file.write(out.toByteArray());
            }
            TxInfo txInfo = loggedTransactions.remove(txId);
            if (commit) {
                synchronized (readFile) {
                    long position = txInfo.position;
                    for (WriteItem wi : txInfo.writeItems) {
                        readChannel.position(position + wi.positionOffset);
                        memoryNode.applyWrite(readChannel, wi.address,
                                wi.offset, wi.length);
                    }
                }
            }
        } catch (IOException e) {
            // FIXME add IOException to method signature?
            log.warn("Unable to decide transaction with id: " + txId, e);
        }
    }

    //--------------------------< Closeable >----------------------------------

    @Override
    public void close() throws IOException {
        checkpoint();
        shutdown.set(true);
        try {
            syncThread.join();
        } catch (InterruptedException e) {
            // ignore
        }
        file.close();
        readFile.close();
        log.info("Sync count: {}", syncCount);
    }


    //--------------------------< FileRedoLog >--------------------------------

    boolean containsUndecidedTransaction(String txId) {
        return loggedTransactions.containsKey(txId);
    }

    long getLength() throws IOException {
        return file.getFilePointer();
    }

    protected void checkpoint() throws IOException {
        memoryNode.sync();
    }

    void closeAndDelete() throws IOException {
        close();
        Files.delete(new File(path).toPath());
    }

    private void runRecovery() throws IOException {
        if (file.length() == 0) {
            return;
        }
        FileInputStream fileIn = new FileInputStream(file.getFD());
        try {
            CountingInputStream countingIn = new CountingInputStream(
                    new BufferedInputStream(fileIn));
            DataInputStream in = new DataInputStream(countingIn);
            long position = 0;
            try {
                for (;;) {
                    position = countingIn.getCount();
                    int recordType = in.readInt();
                    String txId = in.readUTF();
                    if (recordType == RECORD_TYPE_WRITE) {
                        TxInfo txInfo = readTxInfo(in);
                        txInfo.position = position;
                        loggedTransactions.put(txId, txInfo);
                    } else if (recordType == RECORD_TYPE_ABORT) {
                        loggedTransactions.remove(txId);
                    } else if (recordType == RECORD_TYPE_COMMIT) {
                        TxInfo txInfo = loggedTransactions.remove(txId);
                        for (WriteItem wi : txInfo.writeItems) {
                            readChannel.position(txInfo.position + wi.positionOffset);
                            memoryNode.applyWrite(readChannel, wi.address,
                                    wi.offset, wi.length);
                        }
                    } else {
                        log.error("Unknown record type: {}", recordType);
                    }
                }
            } catch (EOFException e) {
                // truncate log file
                file.setLength(position);

            }
        } finally {
            fileIn.close();
        }
    }

    private TxInfo readTxInfo(DataInputStream in) throws IOException {
        TxInfo txInfo = new TxInfo();
        int positionOffset = 0;
        int numMemoryNodes = in.readInt();
        positionOffset += 4;
        for (int i = 0; i < numMemoryNodes; i++) {
            in.readInt();
            positionOffset += 4;
        }
        int numWriteItems = in.readInt();
        positionOffset += 4;
        for (int i = 0; i < numWriteItems; i++) {
            int address = in.readInt();
            positionOffset += 4;
            int offset = in.readInt();
            positionOffset += 4;
            int dataSize = in.readInt();
            positionOffset += 4;
            txInfo.writeItems.add(new WriteItem(address, offset, positionOffset, dataSize));
            in.skipBytes(dataSize);
            positionOffset += dataSize;
        }
        return txInfo;
    }

    //------------------------------< TxInfo >---------------------------------

    private static final class TxInfo {

        long position;

        final List<WriteItem> writeItems = new ArrayList<>();
    }

    private static final class WriteItem {

        final int address;

        final int offset;

        final int positionOffset;

        final int length;

        WriteItem(int address, int offset, int positionOffset, int length) {
            this.address = address;
            this.offset = offset;
            this.positionOffset = positionOffset;
            this.length = length;
        }
    }

}

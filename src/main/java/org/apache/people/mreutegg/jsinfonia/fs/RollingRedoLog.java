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

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.people.mreutegg.jsinfonia.Item;
import org.apache.people.mreutegg.jsinfonia.RedoLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RollingRedoLog implements RedoLog, Closeable {

    private static final Logger log = LoggerFactory.getLogger(RollingRedoLog.class);

    private static long MAX_LOG_SIZE = 1024 * 1024 * 128; // 128 MB

    private static final String SUFFIX_FORMAT = "%0" + Long.valueOf(Long.MAX_VALUE).toString().length() + "d";

    private final LinkedList<FileRedoLog> redoLogs = new LinkedList<FileRedoLog>();

    private long nextLogIndex = 0;

    private final FileMemoryNode memoryNode;

    private final File file;

    private final AtomicBoolean stopCleaner = new AtomicBoolean(false);

    private final Thread logCleaner;

    public RollingRedoLog(FileMemoryNode memoryNode, final File file) throws IOException {
        if (!file.isAbsolute()) {
            throw new IllegalArgumentException("file must be absolute");
        }
        this.memoryNode = memoryNode;
        this.file = file;
        File dir = file.getParentFile();
        File[] logFiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(file.getName() + ".");
            }
        });
        Arrays.sort(logFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                Integer logIdx1 = Integer.parseInt(o1.getName().substring(
                        file.getName().length() + 1));
                Integer logIdx2 = Integer.parseInt(o2.getName().substring(
                        file.getName().length() + 1));
                nextLogIndex = Math.max(nextLogIndex, logIdx1);
                nextLogIndex = Math.max(nextLogIndex, logIdx2);
                return logIdx1.compareTo(logIdx2);
            }

        });
        for (File logFile : logFiles) {
            redoLogs.add(new FileRedoLog(memoryNode, logFile));
        }
        if (redoLogs.isEmpty()) {
            addRedoLog();
        }
        this.logCleaner = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!stopCleaner.get()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    try {
                        maybeCleanupLogs();
                    } catch (IOException e) {
                        log.warn("exception cleaning up logs", e);
                    }
                }
            }

        }, "RollingRedoLog Cleaner");
        this.logCleaner.start();
    }

    //-----------------------------< RedoLog >---------------------------------

    @Override
    public void close() throws IOException {
        stopCleaner.set(true);
        try {
            logCleaner.join();
        } catch (InterruptedException e) {
            log.warn("interrupted while waiting for log cleaner thread");
        }
        synchronized (redoLogs) {
            for (FileRedoLog log : redoLogs) {
                log.close();
            }
            redoLogs.clear();
        }
    }

    @Override
    public void append(String txId, List<Item> writeItems,
            Set<Integer> memoryNodeIds) throws IOException {
        FileRedoLog log;
        synchronized (redoLogs) {
            log = redoLogs.peekLast();
        }
        log.append(txId, writeItems, memoryNodeIds);
        if (log.getLength() > MAX_LOG_SIZE) {
            synchronized (redoLogs) {
                log = redoLogs.peekLast();
                if (log.getLength() > MAX_LOG_SIZE) {
                    addRedoLog();
                }
            }
        }
    }

    @Override
    public Set<String> getTransactionIDs() {
        FileRedoLog[] logs;
        synchronized (redoLogs) {
            logs = redoLogs.toArray(new FileRedoLog[redoLogs.size()]);
        }
        Set<String> ids = new HashSet<String>();
        for (FileRedoLog log : logs) {
            ids.addAll(log.getTransactionIDs());
        }
        return ids;
    }

    @Override
    public void decided(String txId, boolean commit) {
        FileRedoLog log = null;
        synchronized (redoLogs) {
            Iterator<FileRedoLog> logs = redoLogs.descendingIterator();
            while (logs.hasNext()) {
                FileRedoLog frl = logs.next();
                if (frl.containsUndecidedTransaction(txId)) {
                    log = frl;
                    break;
                }
            }
        }
        if (log == null) {
            throw new IllegalArgumentException("unknown transaction id: " + txId);
        }
        log.decided(txId, commit);
    }

    //-----------------------------< RollingRedoLog >--------------------------

    private void addRedoLog() throws IOException {
        StringBuilder sb = new StringBuilder();
        new Formatter(sb).format(SUFFIX_FORMAT, nextLogIndex++);
        redoLogs.add(new FileRedoLog(memoryNode, 
                new File(file.getParentFile(), file.getName() + "." + sb)));
    }

    private void maybeCleanupLogs() throws IOException {
        for (;;) {
            FileRedoLog log;
            synchronized (redoLogs) {
                if (redoLogs.size() <= 1) {
                    return;
                }
                log = redoLogs.peekFirst();
            }
            if (log.getTransactionIDs().isEmpty()) {
                log.closeAndDelete();
            }
            synchronized (redoLogs) {
                redoLogs.removeFirst();
            }
        }
    }

}

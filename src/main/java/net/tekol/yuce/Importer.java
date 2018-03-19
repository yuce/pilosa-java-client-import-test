/*
 * Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

package net.tekol.yuce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pilosa.client.*;
import com.pilosa.client.orm.Frame;
import com.pilosa.client.orm.Index;
import com.pilosa.client.orm.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class Importer {
    public static void main(String...args) {
        if (args.length < 5) {
            System.err.println("Usage: %prog pilosa_addr max_row_id max_col_id batch_size random|ordered [thread_count] [slice_width]");
            System.exit(1);
        }

        for (String arg : args) {
            logger.debug("ARG: " + arg);
        }

        final String pilosaAddr = args[0];
        long maxRowID = Long.parseLong(args[1]);
        long maxColumnID = Long.parseLong(args[2]);
        final int batchSize = Integer.parseInt(args[3]);
        final String strategy = args[4];
        final int cpuCount = Runtime.getRuntime().availableProcessors();
        final int importThreadCount = (args.length >= 6)? Integer.parseInt(args[5]) : cpuCount;
        BitIterator iterator = null;
        final int resolutionMs = 60 * 1000; // 60 seconds
        long sliceWidth = (args.length >= 7)? Long.parseLong(args[6]) : 0;

        switch (strategy) {
            case "random":
                iterator = new XBitIterator(maxRowID, maxColumnID);
                break;
            case "ordered":
                iterator = new NBitIterator(maxRowID);
                break;
            default:
                System.err.println("Strategy must be one of: random, ordered");
                System.exit(1);
        }

        final BlockingQueue<ImportStatusUpdate> statusQueue = new LinkedBlockingDeque<>(1000);
        final ImportMonitor monitor = new ImportMonitor(statusQueue, resolutionMs);
        final Thread monitorThread = new Thread(monitor);
        monitorThread.setDaemon(true);
        monitorThread.start();

        logger.info("Pilosa addr:         " + pilosaAddr);
        logger.info("Max Row ID:          " + maxRowID);
        logger.info("Max Column ID:       " + maxColumnID);
        logger.info("Batch size:          " + batchSize);
        logger.info("Generation strategy: " + strategy);
        logger.info("CPU count:           " + cpuCount);
        logger.info("Import thread count: " + importThreadCount);
        logger.info("Slice width:         " + sliceWidth);

        final long tic = System.nanoTime();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            monitorThread.interrupt();
            end(monitor, tic);
        }));

        Cluster cluster = Cluster.withHost(URI.address(pilosaAddr));
        ClientOptions.Builder builder = ClientOptions.builder()
                .setImportThreadCount(importThreadCount);
        if (sliceWidth > 0) {
            builder.setSliceWidth(sliceWidth);
        }
        ClientOptions options = builder.build();
        PilosaClient client = PilosaClient.withCluster(cluster, options);
        Schema schema = client.readSchema();
        Index index = schema.index("i1");
        Frame frame = index.frame("f1");
        client.syncSchema(schema);

        client.importFrame(frame, iterator, batchSize, statusQueue);
    }

    private static void end(ImportMonitor monitor, long tic) {
        long tac = System.nanoTime();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(tac - tic);
        BitTimeBucket bitTimeBucket = monitor.getBitTimeBucket();
        bitTimeBucket.end();
        logger.info(String.format("Took %d ms to import %d bits", elapsedMs, bitTimeBucket.getBitCount()));
        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(System.err, bitTimeBucket.getBuckets());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private final static Logger logger = LoggerFactory.getLogger(Importer.class);
}

class XBitIterator implements BitIterator {

    XBitIterator(final long maxRowID, final long maxColumnID) {
        this.bitCount = maxRowID * maxColumnID;
        this.maxRowID = maxRowID;
        this.maxColumnID = maxColumnID;
    }

    public boolean hasNext() {
        return this.bitCount > 0;
    }

    public Bit next() {
        this.bitCount--;
        long rowID = (long)(Math.random() * this.maxRowID);
        long columnID = (long)(Math.random() * this.maxColumnID);
        return Bit.create(rowID, columnID);
    }

    private final long maxRowID;
    private final long maxColumnID;
    private long bitCount;
}

class NBitIterator implements BitIterator {

    NBitIterator(long maxID) {
        this.maxID = maxID;
    }

    public boolean hasNext() {
        return this.currentID < this.maxID;
    }

    public Bit next() {
        this.currentID++;
        long rowID = this.currentID;
        long columnID = this.currentID;
        return Bit.create(rowID, columnID);
    }

    private long maxID;
    private long currentID = 0;
}

class ImportMonitor implements Runnable {
    @Override
    public void run() {
        System.err.println("MONITOR STARTED");
        this.bitTimeBucket.start();

        while (true) {
            try {
                ImportStatusUpdate statusUpdate = this.statusQueue.take();
                this.bitTimeBucket.add(statusUpdate.getImportedCount());
                System.err.println(String.format("TOTAL IMPORTED:%d %s", this.bitTimeBucket.getBitCount(), statusUpdate));
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public BitTimeBucket getBitTimeBucket() {
        return this.bitTimeBucket;
    }

    ImportMonitor(final BlockingQueue<ImportStatusUpdate> statusQueue, final int resolutionMs) {
        this.statusQueue = statusQueue;
        this.bitTimeBucket = new BitTimeBucket(resolutionMs, 12);
    }

    private final BlockingQueue<ImportStatusUpdate> statusQueue;
    private BitTimeBucket bitTimeBucket;
}

final class BitTimeBucket {
    public List<Long> getBuckets() {
        int bucketCount = (int)Math.ceil((this.endTime - this.startTime) / (double)this.resolution);
        if (bucketCount >= this.bitsPerTimeBucket.size()) {
            return this.bitsPerTimeBucket;
        }
        return this.bitsPerTimeBucket.subList(0, bucketCount);
    }

    public long getBitCount() {
        return this.bitCount;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public long getEndTime() {
        return this.endTime;
    }

    public long totalTime() {
        return this.endTime - this.startTime;
    }

    public void add(long count) {
        final int timeBucket = (int)((System.currentTimeMillis() - this.startTime) / this.resolution);
        this.bitsPerTimeBucket.set(timeBucket, this.bitsPerTimeBucket.get(timeBucket) + count);
        this.bitCount += count;
    }

    public void start() {
        this.startTime = System.currentTimeMillis();
        this.endTime = this.startTime;
    }

    public synchronized void end() {
        this.endTime = System.currentTimeMillis();
    }

    BitTimeBucket(int resolutionMs, int maxHours) {
        this.resolution = resolutionMs;
        this.initTimeBucket(maxHours);
    }

    private void initTimeBucket(final int maxHours) {
        final int msPerHour = 60 * 60 * 1000;
        final int bucketCount = (int)(Math.ceil(maxHours * msPerHour / this.resolution));
        this.bitsPerTimeBucket = new ArrayList<>(bucketCount); // 12 hours
        for (int i = 0; i < bucketCount; i++) {
            this.bitsPerTimeBucket.add(0L);
        }
    }

    final private int resolution;
    private long startTime = 0;
    private long endTime = 0;
    private List<Long> bitsPerTimeBucket;
    private long bitCount = 0;

}
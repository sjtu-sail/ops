/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import java.util.LinkedList;
import java.util.HashMap;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.OpsPointer;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;
import org.apache.spark.MapOutputTracker;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format
 * that can be served / consumed via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 * <p>
 * This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 * <ul>
 *    <li>no Ordering is specified,</li>
 *    <li>no Aggregator is specified, and</li>
 *    <li>the number of partitions is less than
 *      <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 * <p>
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 */
final class OpsSharedMasterWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(OpsSharedMasterWriter.class);

  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final TaskMemoryManager memoryManager;
  private final BlockManager blockManager;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  private final int mapId;
  private final Serializer serializer;
  private final IndexShuffleBlockResolver shuffleBlockResolver;

  private final MapOutputTracker mapOutputTracker;
  private final String executorId;
  private final int totalMapsNum;

  /** Array of file writers, one for each partition */
  private DiskBlockObjectWriter[] partitionWriters;
  private FileSegment[] partitionWriterSegments;
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;

  private int numWriters = 5;
  private HashMap<Integer, LinkedList<MemoryBlock>> pendingPages = new HashMap<>();

  // OPS log
  private TaskMetrics taskMetrics;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  OpsSharedMasterWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      SerializedShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf conf,
      MapOutputTracker tracker,
      String executorId,
      int mapsNum) {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.taskMetrics = taskContext.taskMetrics();
    this.memoryManager = taskContext.taskMemoryManager();
    this.serializer = dep.serializer();
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.mapOutputTracker = tracker;
    this.executorId = executorId;
    this.totalMapsNum = mapsNum;

    for (int i = 0; i < this.numWriters; i++) {
      this.pendingPages.put(i, new LinkedList<>());
    }
  }

  private int partitionToNum(int partitionId) {
    return partitionId % this.numWriters;
  }

  public void writePage(MemoryBlock page) {
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    final int diskWriteBufferSize = 1024 * 1024;
    final byte[] writeBuffer = new byte[diskWriteBufferSize];

    for (OpsPointer pointer : page.pointers) {
      DiskBlockObjectWriter writer = partitionWriters[pointer.partitionId];
      final Object recordPage = page.getBaseObject();
      final long recordOffsetInPage = pointer.pageOffset;
      // int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
      long dataRemaining = pointer.length;
      long recordReadPosition = recordOffsetInPage;
      while (dataRemaining > 0) {
        final int toTransfer = (int)Math.min(diskWriteBufferSize, dataRemaining);
        Platform.copyMemory(
          recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
        writer.write(writeBuffer, 0, toTransfer);
        recordReadPosition += toTransfer;
        dataRemaining -= toTransfer;
      }
      writer.recordWritten();
    }
  }

  public synchronized void addPages(List<MemoryBlock> pages) {
    for (MemoryBlock page : pages) {
      int id = partitionToNum(page.partitionId);
      this.pendingPages.get(id).add(page);
    }
    notifyAll();
  }

  public synchronized MemoryBlock getPage(int id) throws InterruptedException {
    LinkedList<MemoryBlock> pendingList = this.pendingPages.get(id);
    while(pendingList.isEmpty()) {
      wait();
    }
    return pendingList.removeFirst();
  }

  public void freeSharedPage(MemoryBlock page) {
    memoryManager.freeSharedPage(page);
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    // OPS log
    Long start = System.currentTimeMillis();
    this.taskMetrics.setOpsSortStart(start);

    final SerializerInstance serInstance = serializer.newInstance();
    final long openStartTime = System.nanoTime();
    partitionWriters = new DiskBlockObjectWriter[numPartitions];
    partitionWriterSegments = new FileSegment[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
        blockManager.diskBlockManager().createTempShuffleBlock();
      final File file = tempShuffleBlockIdPlusFile._2();
      final BlockId blockId = tempShuffleBlockIdPlusFile._1();
      // final File file = shuffleBlockResolver.getDataFile(shuffleId, mapId, i);
      // final BlockId blockId = new ShuffleDataBlockId(shuffleId, mapId, i);
      partitionWriters[i] =
        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
    }
    // Creating the file to write to and creating a disk writer both involve interacting with
    // the disk, and can take a long time in aggregate when we open many files, so should be
    // included in the shuffle write time.
    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

    OpsPageWriter<K, V>[] pageWriters = new OpsPageWriter[numWriters];
    for (int i = 0; i < numWriters; i++) {
      pageWriters[i] = new OpsPageWriter<K, V>(this, i);
      pageWriters[i].start();
    }

    int pageNum = 0;
    boolean isLast = false;
    while (true) {
      // memoryManager.testSharedPages();
      int completedMaps = this.mapOutputTracker.syncMapSize(this.shuffleId, this.executorId);
      if (completedMaps >= this.totalMapsNum) {
        System.out.println("The last time to getSharedPages, all maps done");
        isLast = true;
      }

      List<MemoryBlock> pages = memoryManager.getSharedPages();
      int pagesSize = pages.size();
      System.out.println("Get shared pages size: " + pages.size() + ", time: " + System.currentTimeMillis()/1000);
      pageNum += pages.size();
      addPages(pages);
      
      if (isLast) {
        break;
      }
      // if (pagesSize == 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
        // continue;
      // }
    }

    // wait for all pages done
    while (true) {
      boolean isEmpty = true;
      for (int i = 0; i < this.numWriters; i++) {
        if (!this.pendingPages.get(i).isEmpty()) {
          isEmpty = false;
          break;
        }
      }
      if (isEmpty) {
        break;
      }
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    for (int i = 0; i < numWriters; i++) {
      pageWriters[i].shutDown();
    }

    System.out.println("Pre-merge total page num: " + pageNum);
    
    for (int i = 0; i < numPartitions; i++) {
      final DiskBlockObjectWriter writer = partitionWriters[i];
      partitionWriterSegments[i] = writer.commitAndGet();
      // System.out.println("Commit partitionWriterSegments[i]: " + partitionWriterSegments[i].toString());
      writer.close();
    }

    long startMerge = System.currentTimeMillis();
    
    // File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    // File tmp = Utils.tempFileWith(output);
    // try {
    //   partitionLengths = writePartitionedFile(tmp);
    //   shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
    // } finally {
    //   if (tmp.exists() && !tmp.delete()) {
    //     logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
    //   }
    // }

    final long[] partitionLengths = new long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      partitionLengths[i] = partitionWriterSegments[i].file().length();
        final File outputFile = shuffleBlockResolver.getDataFile(shuffleId, mapId, i);
        Files.move(partitionWriterSegments[i].file(), outputFile);
    }

    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);

    long mergeTime = System.currentTimeMillis() - startMerge;
    long totalTime = System.currentTimeMillis() - start;
    System.out.println("Map Master: " + mergeTime + "-" + totalTime);
    // OPS log
    this.taskMetrics.incOpsSpillTime(System.currentTimeMillis() - start);
  }

  @VisibleForTesting
  long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedFile(File outputFile) throws IOException {
    // OPS log


    // Track location of the partition starts in the output file
    final long[] lengths = new long[numPartitions];
    if (partitionWriters == null) {
      // We were passed an empty iterator
      return lengths;
    }

    final FileOutputStream out = new FileOutputStream(outputFile, true);
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    try {
      for (int i = 0; i < numPartitions; i++) {
        final File file = partitionWriterSegments[i].file();
        if (file.exists()) {
          final FileInputStream in = new FileInputStream(file);
          boolean copyThrewException = true;
          try {
            lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
            copyThrewException = false;
          } finally {
            Closeables.close(in, copyThrewException);
          }
          if (!file.delete()) {
            logger.error("Unable to delete file for partition {}", i);
          }
        }
      }
      threwException = false;
    } finally {
      Closeables.close(out, threwException);
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
    partitionWriters = null;
    return lengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              File file = writer.revertPartialWritesAndClose();
              if (!file.delete()) {
                logger.error("Error while deleting file {}", file.getAbsolutePath());
              }
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}

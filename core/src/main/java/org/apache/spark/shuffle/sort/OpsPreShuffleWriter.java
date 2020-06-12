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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coreos.jetcd.data.KeyValue;

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

final class OpsPreShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(OpsPreShuffleWriter.class);

  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final TaskMemoryManager memoryManager;
  private final BlockManager blockManager;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final int shuffleId;
  public final int mapId;
  private final Serializer serializer;
  private final IndexShuffleBlockResolver shuffleBlockResolver;

  public final MapOutputTracker mapOutputTracker;
  private final String executorId;
  private final int totalMapsNum;
  public final SparkConf conf;

  /** Array of file writers, one for each partition */
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;

  private final int numWriters;
  private HashMap<Integer, ConcurrentLinkedQueue<MemoryBlock>> pendingPages = new HashMap<>();

  private final List<String> opsWorkers = new ArrayList<>();
  private final OpsTransferer<K, V>[] opsTransferers;

  // OPS log
  private TaskMetrics taskMetrics;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  OpsPreShuffleWriter(
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
    this.conf = conf;

    
    // Init EtcdService
    String tmpStr = conf.get("spark.ops.etcd", "http://127.0.0.1:2379");
    System.out.println("Get spark.ops.etcd: " + tmpStr);
    List<String> endpoints = Arrays.asList(tmpStr.split(";"));
    OpsEtcdService.initClient(endpoints);
    
    // Get opsWorkers ip form etcd
    List<KeyValue> workersKV = OpsEtcdService.getKVs("ops/nodes/worker");
    for (KeyValue keyValue : workersKV) {
      String str = keyValue.getValue().toStringUtf8();
      String prefix = "http://";
      if(str.startsWith(prefix)) {
        str = str.substring(prefix.length());
      }
      this.opsWorkers.add(str);
    }
    if (this.opsWorkers.size() == 0) {
      System.out.println("Workers not found from etcd server. Add \"127.0.0.1:14020\" for default.");
      this.opsWorkers.add("127.0.0.1:14020");
    }

    this.numWriters = this.opsWorkers.size();
    List<Set<Integer>> assignment = schedulePartition(this.numWriters, this.numPartitions);
    this.opsTransferers = new OpsTransferer[this.numWriters];
    for (int i = 0; i < this.numWriters; i++) {
      this.pendingPages.put(i, new ConcurrentLinkedQueue<>());
      // split worker string to ip & port.
      String[] worker = opsWorkers.get(i).split(":");
      assert(worker.length == 2);
      opsTransferers[i] = new OpsTransferer<K, V>(this, i, worker[0], Integer.parseInt(worker[1]), assignment.get(i));
      opsTransferers[i].start();
    }
  }

  private List<Set<Integer>> schedulePartition(int numWriters, int numPartitions) {
    ArrayList<Set<Integer>> ret = new ArrayList<>();
    for(int i = 0; i < numWriters; i++) {
      ret.add(new HashSet<Integer>());
    }
    for(int i = 0; i < numPartitions; i++) {
      ret.get(partitionToNum(i)).add(i);
    }
    return ret;
  }

  private int partitionToNum(int partitionId) {
    return partitionId % this.numWriters;
  }

  public synchronized void addPages(List<MemoryBlock> pages) {
    for (MemoryBlock page : pages) {
      int id = partitionToNum(page.partitionId);
      this.pendingPages.get(id).add(page);
    }
    notifyAll();
  }

  public synchronized MemoryBlock getPage(int id) throws InterruptedException {
    ConcurrentLinkedQueue<MemoryBlock> pendingList = this.pendingPages.get(id);
    while(pendingList.isEmpty()) {
      wait();
    }
    return pendingList.poll();
  }

  public void freeSharedPage(int pageNum) {
    memoryManager.freeSharedPage(pageNum);
  }

  public void freeSharedPage(MemoryBlock page) {
    memoryManager.freeSharedPage(page);
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    // OPS log
    Long start = System.currentTimeMillis();
    this.taskMetrics.setOpsSortStart(start);

    final SerializerInstance serInstance = serializer.newInstance();
    final long openStartTime = System.nanoTime();

    // Creating the file to write to and creating a disk writer both involve interacting with
    // the disk, and can take a long time in aggregate when we open many files, so should be
    // included in the shuffle write time.
    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

    

    int pageNum = 0;
    boolean isLast = false;
    while (true) {
      // memoryManager.testSharedPages();
      int completedMaps = this.mapOutputTracker.syncMapSize(this.shuffleId, this.executorId);
      if (completedMaps >= this.totalMapsNum) {
        System.out.println("The last time to getSharedPages, all maps done: " + completedMaps);
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
    // int count = 0;
    // while (true) {
    //   boolean isEmpty = true;
    //   System.out.println("wait for all pages done, count = " + count );
    //   for (int i = 0; i < this.numWriters; i++) {
    //     if (!this.pendingPages.get(i).isEmpty()) {
    //       isEmpty = false;
    //       break;
    //     }
    //   }
    //   if (isEmpty) {
    //     break;
    //   }
    //   if (count > 5) {
    //     System.out.println("wait for all pages done, can't wait.");
    //     break;
    //   }
    //   count++;
    //   try {
    //     Thread.sleep(3000);
    //   } catch (InterruptedException e) {
    //     e.printStackTrace();
    //   }
    // }
    for (int i = 0; i < numWriters; i++) {
      opsTransferers[i].shutDown();
    }
    System.out.println("Sync shuffle to master");
    this.mapOutputTracker.syncShuffle();

    System.out.println("Pre-merge total page num: " + pageNum);

    long startMerge = System.currentTimeMillis();

    partitionLengths = new long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      // partitionLengths[i] = partitionWriterSegments[i].file().length();
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

        return None$.empty();
      }
    }
  }
}

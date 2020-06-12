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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;

import scala.Tuple2;
import scala.collection.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.OpsPointer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.google.protobuf.ByteString;

final class OpsTransferer<K, V> extends Thread {

  private boolean stopped = false;

  private final OpsPreShuffleWriter<K, V> masterWriter;
  private final int id;
  private final String targetIp;
  private final int targetPort;
  private final ManagedChannel channel;
  private final StreamObserver<Page> requestObserver;
  private final Set<Integer> partitionSet;
  private final HashMap<Integer, String> pathMap;
  private int count = 0;

  public OpsTransferer(OpsPreShuffleWriter<K, V> master, int id, String ip, int port, Set<Integer> partitionSet) {
    this.masterWriter = master;
    this.id = id;
    this.targetIp = ip;
    this.targetPort = port;
    this.partitionSet = partitionSet;
    this.pathMap = new HashMap<>();
    
    for (Integer partitionId : partitionSet) {
      String path = masterWriter.conf.get("spark.ops.tmpDir", "/home/root/tmpOps/") 
          + OpsUtils.getMapOutputPath(
            masterWriter.conf.getAppId(), 
            "0", 
            partitionId);
      this.pathMap.put(partitionId, path);
    }

    System.out.println("OpsTransferer start. Target: " + ip + ":" + port + ". PartitionSet: " + partitionSet);

    channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
    OpsShuffleDataGrpc.OpsShuffleDataStub asyncStub = OpsShuffleDataGrpc.newStub(channel);

    this.requestObserver = asyncStub.transfer(new StreamObserver<Ack>() {

      @Override
      public void onNext(Ack ack) {
        masterWriter.freeSharedPage(ack.getPageNum());
      }
      
      @Override
      public void onError(Throwable t) {
          System.out.println("gRPC error" + t.getMessage());
          // System.out.println("gRPC channel break down. Re-addPendingShuffle.");
          // shuffleHandler.addPendingShuffles(shuffle);
          try {
              channel.shutdown().awaitTermination(100, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
              e.printStackTrace();
              //TODO: handle exception
          }
      }
      
      @Override
      public void onCompleted() {
          System.out.println("Transfer completed.");
      
          // ShuffleCompletedConf shuffleC = new ShuffleCompletedConf(new ShuffleConf(shuffle.getTask(), shuffle.getDstNode(), shuffle.getNum()), hadoopPath);
          // shuffleHandler.addPendingShuffleHandlerTask(new ShuffleHandlerTask(shuffleC));
          try {
              channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
          } catch (Exception e) {
              e.printStackTrace();
              //TODO: handle exception
          }
      }
    });
  }

  @Override
  public void run() {
    try {
      while (!this.stopped && !Thread.currentThread().isInterrupted()) {
        MemoryBlock page = null;
        try {
          // Get a page
          page = this.masterWriter.getPage(id);
          // Shuffle
          shuffle(page);
          // this.masterWriter.freeSharedPage(page);
          page = null;    
        } finally {
          if (page != null) {
            this.masterWriter.freeSharedPage(page);      
          }
        }
      }
    } catch (InterruptedException ie) {
      return;
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void shuffle(MemoryBlock block) {

    if(!this.partitionSet.contains(block.partitionId)) {
      System.err.println("Wrong page: illegal partition id " + block.partitionId);
      return;
    }

    count++;
    if(count > 100) {
      System.out.println("[OPS-log]-sendRequest-" + System.currentTimeMillis() + "-" + this.targetIp);
      count = 0;
    }

    try {
      // LinkedList<Long> offsets = new LinkedList<>();
      // LinkedList<Long> lengths = new LinkedList<>();
      // for (OpsPointer pointer : block.pointers) {
      //   offsets.add(pointer.pageOffset);
      //   lengths.add(pointer.length);
      // }

      ByteArrayOutputStream byteWriter = new ByteArrayOutputStream();
      for (OpsPointer pointer : block.pointers) {
        byte[] writeBuffer = new byte[(int)pointer.length];
        final Object recordPage = block.getBaseObject();
        final long recordOffsetInPage = pointer.pageOffset;
        // int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
        Platform.copyMemory(
            recordPage, pointer.pageOffset, writeBuffer, Platform.BYTE_ARRAY_OFFSET, pointer.length);
        byteWriter.write(writeBuffer, 0, (int)pointer.length);
      }
      byte[] content = byteWriter.toByteArray();
      byteWriter.close();

      String path = this.pathMap.get(block.partitionId);

      Page page = Page.newBuilder()
          .setContent(ByteString.copyFrom(content, 0, content.length))
          .setPageNum(block.pageNumber)
          // .addAllOffsets(offsets)
          // .addAllLengths(lengths)
          .setPath(path).build();
      // logger.debug("Transfer data. Length: " + block.getBaseObject().length);
      this.requestObserver.onNext(page);
      // this.requestObserver.onCompleted();

    } catch (RuntimeException e) {
        // Cancel RPC
        e.printStackTrace();
        this.requestObserver.onError(e);
        throw e;
    } catch (Exception e) {
        e.printStackTrace();
    }
  }

  public void shutDown() {
    this.stopped = true;
    try {
      this.requestObserver.onCompleted();
      // channel.shutdown().awaitTermination(100, TimeUnit.MILLISECONDS);
      commit();
      interrupt();
      join(5000);
    } catch (Exception ie) {
      ie.printStackTrace();
    }
    System.out.println("Transferer shutDown.");
  }

  private void commit() {
    for (String path : this.pathMap.values()) {
      // System.out.println("Commit shuffle: " + targetIp + ": " + path);
      this.masterWriter.mapOutputTracker.commitShuffle(targetIp, path);
    }
  }
}

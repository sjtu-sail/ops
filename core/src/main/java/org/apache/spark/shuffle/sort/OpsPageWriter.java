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

import org.apache.spark.unsafe.memory.MemoryBlock;

final class OpsPageWriter<K, V> extends Thread {

  private boolean stopped = false;

  private final OpsSharedMasterWriter<K, V> masterWriter;
  private final int id;

  public OpsPageWriter(OpsSharedMasterWriter<K, V> master, int id) {
    this.masterWriter = master;
    this.id = id;
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
          this.masterWriter.writePage(page);
          this.masterWriter.freeSharedPage(page);  
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

  public void shutDown() {
    this.stopped = true;
    try {
      interrupt();
      join(5000);
    } catch (Exception ie) {
      ie.printStackTrace();
    }
  }
}

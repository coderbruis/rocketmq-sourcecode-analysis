/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * transient: 表示短暂的
 * 短暂的存储池。底层使用的是DirectByteBuffer堆外内存来存储临时数据，Rocket将消息存储至DirectByteBuffer之后，
 * 再由Commit线层定时将数据从该DirectByteBuffer堆外内存复制到与目标物理文件(commitlog)对应的内存映射中？
 * RocketMQ引入该机制是为了提供一种内存锁定，将当前堆外内存一直锁定在内存中，避免被进程将内存交换到磁盘中。??? 这句待确定
 *
 */
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // availableBuffers个数，可在broker配置文件中通过transientStorePollSize进行设置，默认为5
    private final int poolSize;
    // 每个ByteBuffer大小，默认为mappedFileSizeCommitLog，表明TransientStorePool为CommitLog文件服务
    private final int fileSize;
    // 双端队列，ByteBuffer容器
    private final Deque<ByteBuffer> availableBuffers;
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * 初始往availableBuffers中添加5个堆外内存空间
     *
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            // 分配与 CommitLog 文件大小相同的堆外内存
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 锁定堆外内存，确保不会被置换到虚拟内存中去，以便提高存储性能
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            // 往队列中添加byteBuffer堆外内存对象
            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 释放堆外内存
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    // 获取堆外内存对象
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        // 从双端列表中获取堆外内存对象
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}

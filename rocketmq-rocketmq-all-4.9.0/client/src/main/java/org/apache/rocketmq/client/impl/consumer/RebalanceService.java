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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.debug.DebugUtils;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 负载均衡定时任务，每个20s进行一次负载均衡。RocketMQ队列重新分布由此类来提供。
 */
public class RebalanceService extends ServiceThread {
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));            // 默认20s
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
//        log.info(this.getServiceName() + " service started");
        log.info("BRUIS's LOG: RebalanceService -> service started");

        while (!this.isStopped()) {                     // 默认死循环执行
            this.waitForRunning(DebugUtils.commonTimeoutMillis_60_MINUTES);          // 线程每隔20s就会执行一次
            this.mqClientFactory.doRebalance();
        }

//        log.info(this.getServiceName() + " service end");
        log.info("BRUIS's LOG: RebalanceService -> service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}

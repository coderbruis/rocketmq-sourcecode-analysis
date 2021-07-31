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
package org.apache.rocketmq.remoting;

import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 远程通信Client接口
 */
public interface RemotingClient extends RemotingService {

    /**
     * 更新命名服务器地址列表
     * @param addrs
     */
    void updateNameServerAddressList(final List<String> addrs);

    /**
     * 获取命名服务器地址列表
     * @return
     */
    List<String> getNameServerAddressList();

    /**
     * 同步通信消息发送
     * @param addr          NameServer地址
     * @param request       RemotingCommand请求对象
     * @param timeoutMillis 同步超时时间
     * @return
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    RemotingCommand invokeSync(final String addr, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步通信消息发送
     * @param addr           NameServer地址
     * @param request        RemotingCommand请求对象
     * @param timeoutMillis  同步超时时间
     * @param invokeCallback 回调方法
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 单向通信消息发送
     * @param addr           NameServer地址
     * @param request        RemotingCommand请求对象
     * @param timeoutMillis  同步超时时间
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 用于注册一些底层的通信服务：比如进行元数据信息的同步工作、commitLog的同步等
     * @param requestCode    底层业务通信规则码，详细在org.apache.rocketmq.common.protocol.RequestCode
     * @param processor      注册器（单线程）
     * @param executor       线程池，用于执行注册器的业务逻辑
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    /**
     * 回调函数执行线程设置，用于生产者发送消息后的回调线程池：
     * 比如org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl类里面异步发送消息后回调
     * @param callbackExecutor
     */
    void setCallbackExecutor(final ExecutorService callbackExecutor);

    /**
     * 获取回调池：NameServer地址
     * @return
     */
    ExecutorService getCallbackExecutor();

    boolean isChannelWritable(final String addr);
}

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

package org.apache.dubbo.rpc.protocol.tri.transport;

import org.apache.dubbo.rpc.protocol.tri.command.QueuedCommand;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class WriteQueue {

    static final int DEQUE_CHUNK_SIZE = 128;
    private final Channel channel;
    private final Queue<QueuedCommand> queue;
    private final AtomicBoolean scheduled;

    public WriteQueue(Channel channel) {
        this.channel = channel;
        queue = new ConcurrentLinkedQueue<>();
        scheduled = new AtomicBoolean(false);
    }

    public ChannelPromise enqueue(QueuedCommand command) {
        ChannelPromise promise = command.promise();
        if (promise == null) {
            promise = channel.newPromise();
            command.promise(promise);
        }
        queue.add(command);

        // 单独线程获取queue中的数据进行发送
        scheduleFlush();
        return promise;
    }

    public void scheduleFlush() {
        if (scheduled.compareAndSet(false, true)) {
            // EventLoop是一个线程池，类型为ExecutorService
            channel.eventLoop().execute(this::flush);
        }
    }

    public void close() {
        channel.close();
    }

    private void flush() {
        try {
            QueuedCommand cmd;
            int i = 0;
            boolean flushedOnce = false;
            while ((cmd = queue.poll()) != null) {
                cmd.run(channel);
                i++;

                // 不停的从队列中获取数据，满128个数据就flush一次
                if (i == DEQUE_CHUNK_SIZE) {
                    i = 0;
                    channel.flush();
                    flushedOnce = true;
                }
            }

            // 如果队列中每数据了，则直接flush
            if (i != 0 || !flushedOnce) {
                channel.flush();
            }
        } finally {
            scheduled.set(false);

            // 如果队列中又有数据了，则继续异步flush
            if (!queue.isEmpty()) {
                scheduleFlush();
            }
        }
    }

}

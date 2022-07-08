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

package org.apache.dubbo.rpc.protocol.tri.call;

import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.rpc.TriRpcStatus;

import java.util.Map;

public class ObserverToClientCallListenerAdapter implements ClientCall.Listener {

    private final StreamObserver<Object> delegate;
    private ClientCall call;

    public ObserverToClientCallListenerAdapter(StreamObserver<Object> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onMessage(Object message) {
        // 接收到一个响应结果，回调StreamObserver
        delegate.onNext(message);
        // 继续处理下一个响应结果
        if (call.isAutoRequestN()) {
            call.requestN(1);
        }
    }

    @Override
    public void onClose(TriRpcStatus status, Map<String, Object> trailers) {
        if (status.isOk()) {
            delegate.onCompleted();
        } else {
            delegate.onError(status.asException());
        }
    }

    @Override
    public void onStart(ClientCall call) {
        // 接收到响应头是会执行这个方法
        this.call = call;
        if (call.isAutoRequestN()) {
            call.requestN(1);
        }
    }
}

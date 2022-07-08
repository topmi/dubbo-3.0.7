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
package org.apache.dubbo.springboot.demo.provider;


import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.config.annotation.DubboService;
import org.apache.dubbo.config.annotation.Method;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.springboot.demo.DemoService;

import java.util.concurrent.CompletableFuture;

@DubboService
public class DemoServiceImpl implements DemoService {

    @Override
    public String sayHello(String name) {
        return "Hello " + name + RpcContext.getServerContext().getUrl().getPort();
    }


    // SERVER_STREAM
    @Override
    public void sayHelloServerStream(String name, StreamObserver<String> response) {

        response.onNext(name + " hello");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        response.onNext(name + " world");

        response.onCompleted();

    }

    // CLIENT_STREAM / BI_STREAM
    @Override
    public StreamObserver<String> sayHelloBiStream(StreamObserver<String> response) {
        return new StreamObserver<String>() {
            @Override
            public void onNext(String name) {
                System.out.println(name);
                response.onNext("hello: "+name);
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
                System.out.println("completed");
            }
        };
    }

}

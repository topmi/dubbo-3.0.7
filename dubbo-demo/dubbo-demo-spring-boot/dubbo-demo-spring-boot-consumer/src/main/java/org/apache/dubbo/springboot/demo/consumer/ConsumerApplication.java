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

package org.apache.dubbo.springboot.demo.consumer;

import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.config.annotation.DubboReference;
import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.apache.dubbo.springboot.demo.DemoService;
import org.apache.dubbo.springboot.demo.HelloService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@SpringBootApplication
@Service
@EnableDubbo
@RestController
public class ConsumerApplication {

    @DubboReference
    private DemoService demoService; // DemoService--->应用名-->实例ip+port--->DemoService?  tri://ip+port/DemoService---> TripleInvoker.invoke(Invocation对象)

    @GetMapping("/")
    public String hello() {

//        String result = demoService.sayHello("zhouyu");  // TripleInvoker.invoke(Invocation对象)
//        return result;

        // 服务端流
        demoService.sayHelloServerStream("zhouyu", new ZhouyuResultStreamObserver());
        return "success";


        // 客户端流
//        StreamObserver<String> streamObserver = demoService.sayHelloBiStream(new ZhouyuResultStreamObserver());
//        streamObserver.onNext("zhouyu1");
//        streamObserver.onCompleted();
//        return "success";

    }

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    static class ZhouyuResultStreamObserver implements StreamObserver<String> {


        @Override
        public void onNext(String data) {
            System.out.println(data);
        }

        @Override
        public void onError(Throwable throwable) {
            System.out.println(throwable);
        }

        @Override
        public void onCompleted() {
            System.out.println("complete");
        }
    }
}



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

package org.apache.dubbo.rpc.protocol.tri;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.remoting.api.Connection;
import org.apache.dubbo.remoting.api.ConnectionManager;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.CancellationContext;
import org.apache.dubbo.rpc.FutureContext;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.TimeoutCountDown;
import org.apache.dubbo.rpc.TriRpcStatus;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.PackableMethod;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.StubMethodDescriptor;
import org.apache.dubbo.rpc.protocol.AbstractInvoker;
import org.apache.dubbo.rpc.protocol.tri.call.ClientCall;
import org.apache.dubbo.rpc.protocol.tri.call.ObserverToClientCallListenerAdapter;
import org.apache.dubbo.rpc.protocol.tri.call.UnaryClientCallListener;
import org.apache.dubbo.rpc.protocol.tri.compressor.Compressor;
import org.apache.dubbo.rpc.support.RpcUtils;

import io.netty.util.AsciiString;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.CommonConstants.ENABLE_TIMEOUT_COUNTDOWN_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_ATTACHMENT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

/**
 * TripleInvoker
 */
public class TripleInvoker<T> extends AbstractInvoker<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TripleInvoker.class);


    private final Connection connection;
    private final ReentrantLock destroyLock = new ReentrantLock();
    private final Set<Invoker<?>> invokers;
    private final ExecutorService streamExecutor;
    private final String acceptEncodings;

    public TripleInvoker(Class<T> serviceType,
        URL url,
        String acceptEncodings,
        ConnectionManager connectionManager,
        Set<Invoker<?>> invokers,
        ExecutorService streamExecutor) {
        super(serviceType, url, new String[]{INTERFACE_KEY, GROUP_KEY, TOKEN_KEY});
        this.invokers = invokers;
        // 与服务提供者建立Socket连接
        this.connection = connectionManager.connect(url);
        this.acceptEncodings = acceptEncodings;
        this.streamExecutor = streamExecutor;
    }

    private static AsciiString getSchemeFromUrl(URL url) {
        boolean ssl = url.getParameter(CommonConstants.SSL_ENABLED_KEY, false);
        return ssl ? TripleConstant.HTTPS_SCHEME : TripleConstant.HTTP_SCHEME;
    }

    @Override
    protected Result doInvoke(final Invocation invocation) {

        // 检查Socket连接是否可用，如果不可用并且没有初始化，那就连接服务端创建Socket连接
        if (!connection.isAvailable()) {
            CompletableFuture<AppResponse> future = new CompletableFuture<>();
            RpcException exception = TriRpcStatus.UNAVAILABLE.withDescription(
                    String.format("upstream %s is unavailable", getUrl().getAddress()))
                .asException();
            future.completeExceptionally(exception);
            return new AsyncRpcResult(future, invocation);
        }

        ConsumerModel consumerModel = (ConsumerModel) (invocation.getServiceModel() != null
            ? invocation.getServiceModel() : getUrl().getServiceModel());
        ServiceDescriptor serviceDescriptor = consumerModel.getServiceModel();
        final MethodDescriptor methodDescriptor = serviceDescriptor.getMethod(
            invocation.getMethodName(),
            invocation.getParameterTypes());

        ClientCall call = new ClientCall(connection, streamExecutor,
            getUrl().getOrDefaultFrameworkModel());

        AsyncRpcResult result;
        try {
            switch (methodDescriptor.getRpcType()) {
                case UNARY:
                    result = invokeUnary(methodDescriptor, invocation, call);
                    break;
                case SERVER_STREAM:
                    result = invokeServerStream(methodDescriptor, invocation, call);
                    break;
                case CLIENT_STREAM:
                case BI_STREAM:
                    result = invokeBiOrClientStream(methodDescriptor, invocation, call);
                    break;
                default:
                    throw new IllegalStateException("Can not reach here");
            }
            return result;
        } catch (Throwable t) {
            final TriRpcStatus status = TriRpcStatus.INTERNAL.withCause(t)
                .withDescription("Call aborted cause client exception");
            RpcException e = status.asException();
            try {
                call.cancel("Canceled by error", e);
            } catch (Throwable t1) {
                LOGGER.error("Cancel triple request failed", t1);
            }
            CompletableFuture<AppResponse> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return new AsyncRpcResult(future, invocation);
        }
    }

    AsyncRpcResult invokeServerStream(MethodDescriptor methodDescriptor, Invocation invocation,
        ClientCall call) {
        RequestMetadata request = createRequest(methodDescriptor, invocation, null);

        // 调用的服务如果是服务端流，那第一个参数是请求参数，第二个参数是StreamObserver
        StreamObserver<Object> responseObserver = (StreamObserver<Object>) invocation.getArguments()[1];

        final StreamObserver<Object> requestObserver = streamCall(call, request, responseObserver);

        // 直接把请求参数发送出去
        requestObserver.onNext(invocation.getArguments()[0]);
        requestObserver.onCompleted();

        return new AsyncRpcResult(CompletableFuture.completedFuture(new AppResponse()), invocation);
    }

    AsyncRpcResult invokeBiOrClientStream(MethodDescriptor methodDescriptor, Invocation invocation,
        ClientCall call) {
        final AsyncRpcResult result;
        RequestMetadata request = createRequest(methodDescriptor, invocation, null);
        StreamObserver<Object> responseObserver = (StreamObserver<Object>) invocation.getArguments()[0];
        final StreamObserver<Object> requestObserver = streamCall(call, request, responseObserver);
        result = new AsyncRpcResult(
            CompletableFuture.completedFuture(new AppResponse(requestObserver)), invocation);
        return result;
    }

    StreamObserver<Object> streamCall(ClientCall call,
        RequestMetadata metadata,
        StreamObserver<Object> responseObserver) {
        if (responseObserver instanceof CancelableStreamObserver) {
            final CancellationContext context = new CancellationContext();
            ((CancelableStreamObserver<Object>) responseObserver).setCancellationContext(context);
            context.addListener(context1 -> call.cancel("Canceled by app", null));
        }
        // 把响应StreamObserver包装成ClientCall.Listener，用来处理响应数据
        ObserverToClientCallListenerAdapter listener = new ObserverToClientCallListenerAdapter(
            responseObserver);

        // 返回一个ClientStreamObserver，用来发送数据
        return call.start(metadata, listener);
    }

    AsyncRpcResult invokeUnary(MethodDescriptor methodDescriptor, Invocation invocation,
        ClientCall call) {
        ExecutorService callbackExecutor = getCallbackExecutor(getUrl(), invocation);
        int timeout = calculateTimeout(invocation, invocation.getMethodName());
        invocation.setAttachment(TIMEOUT_KEY, timeout);
        final AsyncRpcResult result;

        DeadlineFuture future = DeadlineFuture.newFuture(getUrl().getPath(),
            methodDescriptor.getMethodName(), getUrl().getAddress(), timeout, callbackExecutor);
        final Object pureArgument;

        // 调用的方法有两个参数，并且第二个参数的类型是StreamObserver
        if (methodDescriptor.getParameterClasses().length == 2
            && methodDescriptor.getParameterClasses()[1].isAssignableFrom(
            StreamObserver.class)) {
            StreamObserver<Object> observer = (StreamObserver<Object>) invocation.getArguments()[1];

            // 当future完成后，回调StreamObserver中的onNext()方法
            future.whenComplete((r, t) -> {
                if (t != null) {
                    observer.onError(t);
                    return;
                }
                if (r.hasException()) {
                    observer.onError(r.getException());
                    return;
                }
                observer.onNext(r.getValue());
                observer.onCompleted();
            });

            // 方法入参
            pureArgument = invocation.getArguments()[0];
            result = new AsyncRpcResult(CompletableFuture.completedFuture(new AppResponse()),
                invocation);
        } else {
            if (methodDescriptor instanceof StubMethodDescriptor) {
                pureArgument = invocation.getArguments()[0];
            } else {
                pureArgument = invocation.getArguments();
            }
            result = new AsyncRpcResult(future, invocation);
            result.setExecutor(callbackExecutor);
            FutureContext.getContext().setCompatibleFuture(future);
        }

        // UnaryClientCallListener接收到响应数据后，会把响应数据AppResponse设置到future中，并触发future的complete
        ClientCall.Listener callListener = new UnaryClientCallListener(future);

        // 请求原数据，对应的就是Http2HeadersFrame
        RequestMetadata request = createRequest(methodDescriptor, invocation, timeout);

        final StreamObserver<Object> requestObserver = call.start(request, callListener);

        // 发送入参，如果请求头没有发送则会发送请求头
        requestObserver.onNext(pureArgument);
        requestObserver.onCompleted();
        return result;
    }

    RequestMetadata createRequest(MethodDescriptor methodDescriptor, Invocation invocation,
        Integer timeout) {
        final String methodName = RpcUtils.getMethodName(invocation);
        Objects.requireNonNull(methodDescriptor,
            "MethodDescriptor not found for" + methodName + " params:" + Arrays.toString(
                invocation.getCompatibleParamSignatures()));
        final RequestMetadata meta = new RequestMetadata();
        final URL url = getUrl();
        if (methodDescriptor instanceof PackableMethod) {
            meta.packableMethod = (PackableMethod) methodDescriptor;
        } else {
            meta.packableMethod = ReflectionPackableMethod.init(methodDescriptor, url);
        }
        meta.method = methodDescriptor;
        meta.scheme = getSchemeFromUrl(url);
        // TODO read compressor from config
        meta.compressor = Compressor.NONE; // Identity.IDENTITY
        meta.address = url.getAddress();
        meta.service = url.getPath();
        meta.group = url.getGroup();
        meta.version = url.getVersion();
        meta.acceptEncoding = acceptEncodings;
        if (timeout != null) {
            meta.timeout = timeout + "m";
        }
        final Map<String, Object> objectAttachments = invocation.getObjectAttachments();
        if (objectAttachments != null) {
            String application = (String) objectAttachments.get(CommonConstants.APPLICATION_KEY);
            if (application == null) {
                application = (String) objectAttachments.get(
                    CommonConstants.REMOTE_APPLICATION_KEY);
            }
            meta.application = application;
            meta.attachments = objectAttachments;
        }
        return meta;
    }

    @Override
    public boolean isAvailable() {
        if (!super.isAvailable()) {
            return false;
        }
        return connection.isAvailable();
    }

    @Override
    public void destroy() {
        // in order to avoid closing a client multiple times, a counter is used in case of connection per jvm, every
        // time when client.close() is called, counter counts down once, and when counter reaches zero, client will be
        // closed.
        if (!super.isDestroyed()) {
            // double check to avoid dup close
            destroyLock.lock();
            try {
                if (super.isDestroyed()) {
                    return;
                }
                super.destroy();
                if (invokers != null) {
                    invokers.remove(this);
                }
                try {
                    connection.release();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }

            } finally {
                destroyLock.unlock();
            }
        }
    }

    private int calculateTimeout(Invocation invocation, String methodName) {
        if (invocation.get(TIMEOUT_KEY) != null) {
            return (int) invocation.get(TIMEOUT_KEY);
        }
        Object countdown = RpcContext.getClientAttachment().getObjectAttachment(TIME_COUNTDOWN_KEY);
        int timeout;
        if (countdown == null) {
            timeout = (int) RpcUtils.getTimeout(getUrl(), methodName,
                RpcContext.getClientAttachment(), 3000);
            if (getUrl().getParameter(ENABLE_TIMEOUT_COUNTDOWN_KEY, false)) {
                invocation.setObjectAttachment(TIMEOUT_ATTACHMENT_KEY,
                    timeout); // pass timeout to remote server
            }
        } else {
            TimeoutCountDown timeoutCountDown = (TimeoutCountDown) countdown;
            timeout = (int) timeoutCountDown.timeRemaining(TimeUnit.MILLISECONDS);
            invocation.setObjectAttachment(TIMEOUT_ATTACHMENT_KEY,
                timeout);// pass timeout to remote server
        }
        return timeout;
    }

}

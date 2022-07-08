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

package org.apache.dubbo.rpc.protocol.tri.frame;

import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.tri.compressor.DeCompressor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

public class TriDecoder implements Deframer {

    // 5个字节，第1个字节表示压缩标记位，后4个字节表示数据长度
    private static final int HEADER_LENGTH = 5;
    private static final int COMPRESSED_FLAG_MASK = 1;
    private static final int RESERVED_MASK = 0xFE;
    private final CompositeByteBuf accumulate = Unpooled.compositeBuffer();
    private final Listener listener;
    private final DeCompressor decompressor;
    private boolean compressedFlag;
    private long pendingDeliveries;
    private boolean inDelivery = false;
    private boolean closing;
    private boolean closed;

    private int requiredLength = HEADER_LENGTH;

    private GrpcDecodeState state = GrpcDecodeState.HEADER;

    public TriDecoder(DeCompressor decompressor, Listener listener) {
        this.decompressor = decompressor;
        this.listener = listener;
    }


    @Override
    public void deframe(ByteBuf data) {
        if (closing || closed) {
            // ignored
            return;
        }

        // 把接收到的字节数据添加到accumulate中
        accumulate.addComponent(true, data);
        deliver();
    }

    public void request(int numMessages) {
        pendingDeliveries += numMessages;
        deliver();
    }

    @Override
    public void close() {
        closing = true;
        deliver();
    }

    private void deliver() {
        // We can have reentrancy here when using a direct executor, triggered by calls to
        // request more messages. This is safe as we simply loop until pendingDelivers = 0

        // 接到请求头，并解析完请求头之后就会调用这个方法
        // 接收到请求体，把数据添加到accumulate之后，也会调用这个方法
        // 这两个步骤是异步，所以可能解析完请求头后，请求体数据已经来了，也可能没来

        // 如果已经在处理响应体了，就直接return
        if (inDelivery) {
            return;
        }
        inDelivery = true;
        try {
            // Process the uncompressed bytes.
            // pendingDeliveries表示会循环几次，等于2就会循环两次
            // 当前循环的前提是需要的字节数已经够了
            while (pendingDeliveries > 0 && hasEnoughBytes()) {
                switch (state) {
                    case HEADER:
                        // 解析压缩标记，需要5个字节
                        // 解析完压缩标记后，state会变为PAYLOAD
                        processHeader();
                        break;
                    case PAYLOAD:
                        // Read the body and deliver the message.
                        // 处理
                        processBody();

                        // Since we've delivered a message, decrement the number of pending
                        // deliveries remaining.
                        pendingDeliveries--;
                        break;
                    default:
                        throw new AssertionError("Invalid state: " + state);
                }
            }
            if (closing) {
                if (!closed) {
                    closed = true;
                    accumulate.clear();
                    accumulate.release();
                    listener.close();
                }
            }
        } finally {
            inDelivery = false;
        }
    }

    // 指定长度（字节数）是否够了
    private boolean hasEnoughBytes() {
        return requiredLength - accumulate.readableBytes() <= 0;
    }

    /**
     * Processes the GRPC compression header which is composed of the compression flag and the outer
     * frame length.
     */
    private void processHeader() {
        // 读1个字节，看压缩标记位，0或1
        int type = accumulate.readUnsignedByte();
        if ((type & RESERVED_MASK) != 0) {
            throw new RpcException("gRPC frame header malformed: reserved bits not zero");
        }
        compressedFlag = (type & COMPRESSED_FLAG_MASK) != 0;

        // 读4个字节，表示请求体字节数
        requiredLength = accumulate.readInt();

        // Continue reading the frame body.
        state = GrpcDecodeState.PAYLOAD;
    }

    /**
     * Processes the GRPC message body, which depending on frame header flags may be compressed.
     */
    private void processBody() {
        // There is no reliable way to get the uncompressed size per message when it's compressed,
        // because the uncompressed bytes are provided through an InputStream whose total size is
        // unknown until all bytes are read, and we don't know when it happens.
        // 如果压缩了，就会利用decompressor进行解压
        byte[] stream = compressedFlag ? getCompressedBody() : getUncompressedBody();

        listener.onRawMessage(stream);

        // 改好状态，以便处理下一个响应头
        // Done with this frame, begin processing the next header.
        state = GrpcDecodeState.HEADER;
        requiredLength = HEADER_LENGTH;
    }

    private byte[] getCompressedBody() {
        final byte[] compressedBody = getUncompressedBody();
        return decompressor.decompress(compressedBody);
    }

    private byte[] getUncompressedBody() {
        byte[] data = new byte[requiredLength];
        accumulate.readBytes(data);
        return data;
    }


    private enum GrpcDecodeState {
        HEADER,
        PAYLOAD
    }

    public interface Listener {

        void onRawMessage(byte[] data);

        void close();

    }

}

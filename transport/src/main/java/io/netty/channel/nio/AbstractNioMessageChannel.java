/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.nio;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.AbstractNioChannel.AbstractNioUnsafe;

import java.io.IOException;
import java.net.PortUnreachableException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link AbstractNioChannel} base class for {@link Channel}s that operate on messages.
 */
public abstract class AbstractNioMessageChannel extends AbstractNioChannel {
    boolean inputShutdown;

    /**
     * @see AbstractNioChannel#AbstractNioChannel(Channel, SelectableChannel, int)
     */
    protected AbstractNioMessageChannel(Channel parent, SelectableChannel ch, int readInterestOp) {
        super(parent, ch, readInterestOp);
    }

    @Override
    protected AbstractNioUnsafe newUnsafe() {
        return new NioMessageUnsafe();
    }

    @Override
    protected void doBeginRead() throws Exception {
        if (inputShutdown) {
            return;
        }
        super.doBeginRead();
    }

    protected boolean continueReading(RecvByteBufAllocator.Handle allocHandle) {
        return allocHandle.continueReading();
    }

    private final class NioMessageUnsafe extends AbstractNioUnsafe {

        private final List<Object> readBuf = new ArrayList<Object>();

        @Override
        // 读取方法，处理接收到的消息
        public void read() {
            // 确保当前线程在事件循环中
            assert eventLoop().inEventLoop();
            // 获取通道配置
            final ChannelConfig config = config();
            // 获取通道管道
            final ChannelPipeline pipeline = pipeline();
            // 获取接收字节缓冲分配器的句柄，以便管理接收的字节缓冲区和消息读取过程
            final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
            // 设置配置
            allocHandle.reset(config);

            boolean closed = false; // 标记通道是否关闭
            Throwable exception = null; // 捕获异常
            try {
                try {
                    // 循环读取消息
                    do {
                        // 从通道读取消息到缓冲区
                        int localRead = doReadMessages(readBuf);
                        if (localRead == 0) {
                            break; // 如果没有读取到消息，退出循环
                        }
                        if (localRead < 0) {
                            closed = true; // 如果读取到负值，标记通道关闭
                            break;
                        }

                        // 增加读取的消息计数
                        allocHandle.incMessagesRead(localRead);
                    } while (continueReading(allocHandle)); // 检查是否继续读取
                } catch (Throwable t) {
                    exception = t; // 捕获异常
                }

                // 获取读取缓冲区的大小
                int size = readBuf.size();
                for (int i = 0; i < size; i ++) {
                    readPending = false; // 标记读取操作已完成
                    // 触发通道读取事件
                    pipeline.fireChannelRead(readBuf.get(i));
                }
                readBuf.clear(); // 清空读取缓冲区
                allocHandle.readComplete(); // 标记读取完成
                pipeline.fireChannelReadComplete(); // 触发读取完成事件

                if (exception != null) {
                    closed = closeOnReadError(exception); // 处理读取错误
                    pipeline.fireExceptionCaught(exception); // 触发异常事件
                }

                if (closed) {
                    inputShutdown = true; // 标记输入关闭
                    if (isOpen()) {
                        close(voidPromise()); // 关闭通道
                    }
                }
            } finally {
                // 检查是否有未处理的读取请求
                // 这可能是由于两种原因：
                // * 用户在 channelRead(...) 方法中调用了 Channel.read() 或 ChannelHandlerContext.read()
                // * 用户在 channelReadComplete(...) 方法中调用了 Channel.read() 或 ChannelHandlerContext.read()
                // 参见：https://github.com/netty/netty/issues/2254
                if (!readPending && !config.isAutoRead()) {
                    removeReadOp(); // 移除读取操作
                }
            }
        }
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        final SelectionKey key = selectionKey();
        final int interestOps = key.interestOps();

        int maxMessagesPerWrite = maxMessagesPerWrite();
        while (maxMessagesPerWrite > 0) {
            Object msg = in.current();
            if (msg == null) {
                break;
            }
            try {
                boolean done = false;
                for (int i = config().getWriteSpinCount() - 1; i >= 0; i--) {
                    if (doWriteMessage(msg, in)) {
                        done = true;
                        break;
                    }
                }

                if (done) {
                    maxMessagesPerWrite--;
                    in.remove();
                } else {
                    break;
                }
            } catch (Exception e) {
                if (continueOnWriteError()) {
                    maxMessagesPerWrite--;
                    in.remove(e);
                } else {
                    throw e;
                }
            }
        }
        if (in.isEmpty()) {
            // Wrote all messages.
            if ((interestOps & SelectionKey.OP_WRITE) != 0) {
                key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
            }
        } else {
            // Did not write all messages.
            if ((interestOps & SelectionKey.OP_WRITE) == 0) {
                key.interestOps(interestOps | SelectionKey.OP_WRITE);
            }
        }
    }

    /**
     * Returns {@code true} if we should continue the write loop on a write error.
     */
    protected boolean continueOnWriteError() {
        return false;
    }

    protected boolean closeOnReadError(Throwable cause) {
        if (!isActive()) {
            // If the channel is not active anymore for whatever reason we should not try to continue reading.
            return true;
        }
        if (cause instanceof PortUnreachableException) {
            return false;
        }
        if (cause instanceof IOException) {
            // ServerChannel should not be closed even on IOException because it can often continue
            // accepting incoming connections. (e.g. too many open files)
            return !(this instanceof ServerChannel);
        }
        return true;
    }

    /**
     * Read messages into the given array and return the amount which was read.
     */
    protected abstract int doReadMessages(List<Object> buf) throws Exception;

    /**
     * Write a message to the underlying {@link java.nio.channels.Channel}.
     *
     * @return {@code true} if and only if the message has been written
     */
    protected abstract boolean doWriteMessage(Object msg, ChannelOutboundBuffer in) throws Exception;
}

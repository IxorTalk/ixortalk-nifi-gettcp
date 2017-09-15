/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ixortalk.nifi.processors.gettcp;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.nifi.logging.ComponentLog;

import java.net.ConnectException;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


@Sharable
public class TcpClientHandler extends SimpleChannelInboundHandler<Object> {

    public static final int POLLING_TIMEOUT_IN_MS = 100;
    private final GetTCP client;
    private final int reconnectDelayInSeconds;
    private byte delimiter;
    private final BlockingQueue<String> socketMessagesReceived = new ArrayBlockingQueue<>(256);
    private ComponentLog log = null;
    StringBuffer message = new StringBuffer();

    public TcpClientHandler(GetTCP client, int reconnectDelayInSeconds, byte delimiter) {
        this.client = client;
        this.reconnectDelayInSeconds = reconnectDelayInSeconds;
        this.delimiter = delimiter;
    }

    /**
     *
     * When we were able to connect to the TCP server, the method below will be called.
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Connected " + ctx.channel().localAddress() + " to " + ctx.channel().remoteAddress());
    }

    public void setLog(ComponentLog log) {
        this.log = log;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof IdleStateEvent)) {
            return;
        }

        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.READER_IDLE) {
            // The connection was OK but there was no traffic for last period.
            log.info("Disconnecting due to no inbound traffic");
            ctx.close();
        }
    }

    /**
     *
     * As soon as Netty detects an error on the channel (read timeout, or connection unavailable), this method will be called.
     * When this happens we disconnect and schedule a reconnect defined by the reconnectDelayInSeconds param.
     *
     * This will also be called when we stop the processor. At that point, we gracefully shutdown the NioEventLoopGroup
     *
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        log.info("Channel marked inactive. Disconnected " + ctx.channel().localAddress() + " from " + ctx.channel().remoteAddress());
        log.info("Sleeping for " + reconnectDelayInSeconds + "s before reconnecting");

        final EventLoop loop = ctx.channel().eventLoop();
        loop.schedule(new TimerTask() {
            @Override
            public void run() {
                log.info("Reconnecting to " + ctx.channel().remoteAddress());
                Bootstrap b = new Bootstrap();
                client.configureBootstrap(b, loop).connect().addListener(new ConnectionListener(b,log,reconnectDelayInSeconds));;
            }
        }, reconnectDelayInSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        if (cause instanceof ConnectException) {
            log.error("Failed to connect.",cause);
        }
        if (cause instanceof ReadTimeoutException) {
            // The connection was OK but there was no traffic for last period.
            log.error("Disconnecting due to no inbound traffic.");
        } else {
            log.error("Disconnecting due to unknown error",cause);
            cause.printStackTrace();
        }
        ctx.close();

    }


    /**
     *
     * The poll method is called from within the Processor onTrigger method.
     * It fetches the message from the internal memory queue
     *
     * @return
     * @throws InterruptedException
     */
    public String poll() throws InterruptedException {
        return socketMessagesReceived.poll(POLLING_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);
    }

    /**
     *
     * When data becomes available on the socket, it will trigger the method below.
     * We'll read from the buffer until we find the delimiter.
     * When we find the delimiter, we'll offer it to the queue so that it can be subsequently polled by the processor.
     * The delimiter will not be offered as part of the message (TODO: make this optional)
     *
     * In case we fail to convert a byte from the buffer we log an error, but continue processing.
     *
     * @param channelHandlerContext
     * @param o
     * @throws Exception
     */
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        ByteBuf in = (ByteBuf) o;


        try {
            while (in.isReadable()) {
                byte b = in.readByte();

                if (log.isTraceEnabled()) {
                    log.trace("Read byte " + ByteUtils.byteToHex(b));
                }

                if (b == delimiter) {
                    if (log.isTraceEnabled()) {
                        log.trace("Found delimiter, offering message : " + message.toString());
                    }

                    socketMessagesReceived.offer(message.toString());
                    message = new StringBuffer();
                } else {
                    try {
                        message.append(new String(new byte[]{b}, "UTF-8"));
                    } catch (Exception e) {
                        log.error("Error occured while converting byte to string.",e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error occured while reading channel",e);
        }
    }
}
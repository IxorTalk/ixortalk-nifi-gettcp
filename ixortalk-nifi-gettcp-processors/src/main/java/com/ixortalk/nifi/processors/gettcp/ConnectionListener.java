package com.ixortalk.nifi.processors.gettcp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import org.apache.nifi.logging.ComponentLog;

import java.util.concurrent.TimeUnit;

public class ConnectionListener implements ChannelFutureListener {

    private final Bootstrap b;
    private final ComponentLog log;
    private int reconnectDelayInSeconds;

    public ConnectionListener(Bootstrap b,ComponentLog log, int reconnectDelayInSeconds ) {
        this.b = b;
        this.log=log;
        this.reconnectDelayInSeconds = reconnectDelayInSeconds;
    }

    /**
     * Whenever we attempt a connection, regardless of the result (success or failure), this method will be called.
     * In case of a failure, we'll attempt to reconnect again (defined by the reconnectDelayInSeconds param).
     *
     * We'll use the Netty bootstrap to attempt to connect again (and use the same ConnectionListener).
     *
     * @param channelFuture
     * @throws Exception
     */
    @Override
    public void operationComplete(ChannelFuture channelFuture) throws Exception {

        final ChannelFutureListener listener = this;
        if (!channelFuture.isSuccess()) {
            log.warn("Channel failure detected by connectionListener. Planning reconnect in " + reconnectDelayInSeconds + " seconds.");
            final EventLoop loop = channelFuture.channel().eventLoop();
            loop.schedule(new Runnable() {
                @Override
                public void run() {
                    log.warn("Reconnecting the TCP connection.");
                    b.connect().addListener(listener);
                }
            }, reconnectDelayInSeconds, TimeUnit.SECONDS);
        }
    }
}
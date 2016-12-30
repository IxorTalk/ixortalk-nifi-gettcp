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

package com.ixortalk.nifi.processors.gettcp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"get", "fetch", "poll", "tcp", "ingest", "source", "input"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Connects over TCP to the provided server. When receiving data this will writes either the" +
        " full receive buffer or messages based on demarcator to the content of a FlowFile. ")
public final class GetTCP extends AbstractProcessor {

    public static final PropertyDescriptor SERVER_ADDRESS = new PropertyDescriptor
            .Builder().name("Server Address")
            .description("The address of the server to connect to")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port to connect to")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECONNECT_DELAY = new PropertyDescriptor
            .Builder().name("Reconnect delay")
            .description("When a channel failure occured, the amount of time in seconds to wait before connecting again")
            .required(false)
            .defaultValue("5")
            .addValidator(StandardValidators.createLongValidator(1, 86400, true))
            .build();

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor
            .Builder().name("TCP Read Timeout")
            .description("Amound of time in seconds to wait for data on the TCP connection. After this period a channel failure is forced, causing a reconnect attempt")
            .required(false)
            .defaultValue("60")
            .addValidator(StandardValidators.createLongValidator(1, 86400, true))
            .build();

    public static final PropertyDescriptor KEEP_ALIVE = new PropertyDescriptor
            .Builder().name("Keep Alive")
            .description("This determines if TCP keep alive is used.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor MSG_DELIMITER = new PropertyDescriptor
            .Builder().name("Message Delimiter")
            .description("Bytesequence used to split flowfiles. when not provided, the flowfiles will be split according to buffer size.")
            .required(false)
            .defaultValue("04")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The relationship that all sucessful messages from the WebSocket will be sent to")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The relationship that all failed messages from the WebSocket will be sent to")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
    * Will ensure that the list of property descriptors is build only once.
    * Will also create a Set of relationships
    */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(SERVER_ADDRESS);
        _propertyDescriptors.add(PORT);
        _propertyDescriptors.add(RECONNECT_DELAY);
        _propertyDescriptors.add(READ_TIMEOUT);
        _propertyDescriptors.add(KEEP_ALIVE);
        _propertyDescriptors.add(MSG_DELIMITER);

        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private TcpClientHandler handler = null;

    private ProcessContext context = null;

    private Bootstrap b;
    private NioEventLoopGroup g;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        byte delim = (byte)Integer.parseInt(context.getProperty(MSG_DELIMITER).getValue(), 16);
        Integer reconnectDelayInSeconds = context.getProperty(RECONNECT_DELAY).evaluateAttributeExpressions().asInteger();

        getLogger().info("Scheduling GetTCP processor with delimiter " + delim + " and reconnectDelayInSeconds " + reconnectDelayInSeconds);
        this.context = context;
        this.handler =  new TcpClientHandler(this, reconnectDelayInSeconds,delim);
        ;

        Runnable nettyConnect = () -> {

            getLogger().info("Connecting Bootstrap");
            g = new NioEventLoopGroup();
            b = configureBootstrap(new Bootstrap(), g);
            b.connect().addListener(new ConnectionListener(b,getLogger(),context.getProperty(RECONNECT_DELAY).evaluateAttributeExpressions().asInteger()));

        };
        new Thread(nettyConnect).start();
    }

    @OnStopped
    public void tearDown() throws ProcessException {
        g.shutdownGracefully();
    }

    Bootstrap configureBootstrap(Bootstrap b, EventLoopGroup g) {
        String host = context.getProperty(SERVER_ADDRESS).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final ComponentLog log = getLogger();

        getLogger().info("Configuring Bootstrap with address " + host + ":" + port);

        handler.setLog(log);
        b.group(g)
                .channel(NioSocketChannel.class)
                .remoteAddress(host, port)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new IdleStateHandler(context.getProperty(READ_TIMEOUT).asInteger(), 0, 0), handler);
                    }
                });

        return b;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        try {
            final ComponentLog log = getLogger();
            final String messages = handler.poll();

            if (messages == null) {
                if (log.isTraceEnabled()) {
                    log.trace("Read : EMPTY");
                }
                return;
            } else {

                if (log.isInfoEnabled()) {
                    log.info("Read msg : " + messages);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Read msg (hex) : " + ByteUtils.byteArrayToHex(messages.getBytes()));
                }
            }

            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, out -> out.write(messages.getBytes()));

            flowFile = session.putAttribute(flowFile, "tcp.sender", context.getProperty(SERVER_ADDRESS).evaluateAttributeExpressions().getValue());
            flowFile = session.putAttribute(flowFile, "tcp.port", context.getProperty(PORT).evaluateAttributeExpressions().getValue());

            session.transfer(flowFile, REL_SUCCESS);

        } catch (InterruptedException exception){
            throw new ProcessException(exception);
        }
    }
}

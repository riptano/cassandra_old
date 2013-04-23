
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;

public abstract class AbstractPipelineFactory implements ChannelPipelineFactory
{

    // Constants to identify the default handlers
    public static final String FRAME_ENCODER = "frameEncoder";
    public static final String FRAME_DECODER = "frameDecoder";
    public static final String FRAME_COMPRESSOR = "frameCompressor";
    public static final String FRAME_DECOMPRESSOR = "frameDecompressor";
    public static final String MESSAGE_DECODER = "messageDecoder";
    public static final String MESSAGE_ENCODER = "messageEncoder";
    public static final String EXECUTOR = "executor";
    public static final String DISPATCHER = "dispatcher";

    // Stateless handlers
    public static final Message.ProtocolDecoder messageDecoder    = new Message.ProtocolDecoder();
    public static final Message.ProtocolEncoder messageEncoder    = new Message.ProtocolEncoder();
    public static final Frame.Decompressor      frameDecompressor = new Frame.Decompressor();
    public static final Frame.Compressor        frameCompressor   = new Frame.Compressor();
    public static final Frame.Encoder           frameEncoder      = new Frame.Encoder();
    public static final Message.Dispatcher      dispatcher        = new Message.Dispatcher();

    private final ExecutionHandler executionHandler;
    private final Server.ConnectionTracker connectionTracker;

    public AbstractPipelineFactory(Server.ConnectionTracker connectionTracker)
    {
        assert connectionTracker != null : "ConnectionTracker must not be null";

        this.connectionTracker = connectionTracker;
        executionHandler = new ExecutionHandler(new RequestThreadPoolExecutor());
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();

        // add the mandatory handlers
        pipeline.addLast(FRAME_DECODER, new Frame.Decoder(connectionTracker, ServerConnection.FACTORY));
        pipeline.addLast(FRAME_ENCODER, frameEncoder);

        pipeline.addLast(FRAME_DECOMPRESSOR, frameDecompressor);
        pipeline.addLast(FRAME_COMPRESSOR, frameCompressor);

        pipeline.addLast(MESSAGE_ENCODER, messageDecoder);
        pipeline.addLast(MESSAGE_DECODER, messageEncoder);

        pipeline.addLast(EXECUTOR, executionHandler);
        pipeline.addLast(DISPATCHER, dispatcher);

        updatePipeline(pipeline);

        return pipeline;
    }

    /*
     * Implemented by concrete subclasses to add ChannelHandlers to the head of the pipeline
     */
    public abstract void updatePipeline(ChannelPipeline pipeline) throws Exception;
}

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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.collect.Maps;
import com.sun.security.auth.module.Krb5LoginModule;
import org.apache.cassandra.transport.messages.SaslCompleteMessage;
import org.apache.cassandra.transport.messages.SaslTokenRequestMessage;
import org.apache.cassandra.transport.messages.SaslTokenResponseMessage;
import org.apache.cassandra.transport.sasl.client.SaslAuthClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.transport.messages.CredentialsMessage;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import static org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions;

public class SimpleClient
{
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    private static final Logger logger = LoggerFactory.getLogger(SimpleClient.class);
    public final String host;
    public final int port;
    private final ClientEncryptionOptions encryptionOptions;

    protected final ResponseHandler responseHandler = new ResponseHandler();
    protected final Connection.Tracker tracker = new ConnectionTracker();
    protected final Connection connection = new Connection(tracker);
    protected ClientBootstrap bootstrap;
    protected Channel channel;
    protected ChannelFuture lastWriteFuture;

    private final Connection.Factory connectionFactory = new Connection.Factory()
    {
        public Connection newConnection(Connection.Tracker tracker)
        {
            return connection;
        }
    };

    public SimpleClient(String host, int port, ClientEncryptionOptions encryptionOptions)
    {
        this.host = host;
        this.port = port;
        this.encryptionOptions = encryptionOptions;
    }

    public SimpleClient(String host, int port)
    {
        this(host, port, new ClientEncryptionOptions());
    }

    public void connect(boolean useCompression) throws IOException
    {
        establishConnection();

        Map<String, String> options = new HashMap<String, String>();
        options.put(StartupMessage.CQL_VERSION, "3.0.0");
        if (useCompression)
        {
            options.put(StartupMessage.COMPRESSION, "snappy");
            connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
        }
        execute(new StartupMessage(options));
    }

    protected void establishConnection() throws IOException
    {
        // Configure the client.
        bootstrap = new ClientBootstrap(
                        new NioClientSocketChannelFactory(
                            Executors.newCachedThreadPool(),
                            Executors.newCachedThreadPool()));

        bootstrap.setOption("tcpNoDelay", true);

        // Configure the pipeline factory.
        if(encryptionOptions.enabled)
        {
            bootstrap.setPipelineFactory(new SecurePipelineFactory());
        }
        else
        {
            bootstrap.setPipelineFactory(new PipelineFactory());
        }
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess())
        {
            bootstrap.releaseExternalResources();
            throw new IOException("Connection Error", future.getCause());
        }
    }

    public SaslTokenRequestMessage initiateSaslAuthentication(final Map<String, String> credentials)
    {
        SaslAuthClient saslAuthClient = SaslAuthClient.SASL_NETTY_CLIENT.get(channel);
        if(saslAuthClient ==null)
        {
            CallbackHandler cbh = new CallbackHandler()
            {
                @Override
                public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
                {
                }
            };

            logger.debug("Initialising authentication for channel: " + channel);

            saslAuthClient = new SaslAuthClient(getSubject(),
                                                "GSSAPI",
                                                System.getenv("KRB_PROTOCOL"),
                                                System.getenv("KRB_HOST"),
                                                cbh);
            SaslAuthClient.SASL_NETTY_CLIENT.set(channel, saslAuthClient);
        }

        try {
            if (!saslAuthClient.isComplete()) {
                logger.debug("Waiting for authentication to complete..");

                SaslTokenRequestMessage saslTokenMessage = saslAuthClient.firstToken();
                logger.info("Got initial sasl token message, writing to channel");
                return saslTokenMessage;
            }
        } catch (IOException e) {
            logger.error("Failed to authenticate with server due to error: " + e, e);
        }
        return null;
    }

    public void login(Map<String, String> credentials)
    {
        CredentialsMessage msg = new CredentialsMessage();
        msg.credentials.putAll(credentials);
        execute(msg);
    }

    public ResultMessage execute(String query, ConsistencyLevel consistency)
    {
        Message.Response msg = execute(new QueryMessage(query, consistency));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public ResultMessage.Prepared prepare(String query)
    {
        Message.Response msg = execute(new PrepareMessage(query));
        assert msg instanceof ResultMessage.Prepared;
        return (ResultMessage.Prepared)msg;
    }

    public ResultMessage executePrepared(byte[] statementId, List<ByteBuffer> values, ConsistencyLevel consistency)
    {
        Message.Response msg = execute(new ExecuteMessage(statementId, values, consistency));
        assert msg instanceof ResultMessage;
        return (ResultMessage)msg;
    }

    public void close()
    {
        // Wait until all messages are flushed before closing the channel.
        if (lastWriteFuture != null)
            lastWriteFuture.awaitUninterruptibly();

        // Close the connection.  Make sure the close operation ends because
        // all I/O operations are asynchronous in Netty.
        channel.close().awaitUninterruptibly();

        // Shut down all thread pools to exit.
        bootstrap.releaseExternalResources();
    }

    protected Message.Response execute(Message.Request request)
    {
        try
        {
            request.attach(connection);
            lastWriteFuture = channel.write(request);
            Message.Response msg = responseHandler.responses.take();
            if (msg instanceof ErrorMessage)
                throw new RuntimeException((Throwable)((ErrorMessage)msg).error);
            return msg;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    // Stateless handlers
    private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
    private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
    private static final Frame.Decompressor frameDecompressor = new Frame.Decompressor();
    private static final Frame.Compressor frameCompressor = new Frame.Compressor();
    private static final Frame.Encoder frameEncoder = new Frame.Encoder();

    private static class ConnectionTracker implements Connection.Tracker
    {
        public void addConnection(Channel ch, Connection connection) {}
        public void closeAll() {}
    }

    private class PipelineFactory implements ChannelPipelineFactory
    {
        public ChannelPipeline getPipeline() throws Exception
        {
            ChannelPipeline pipeline = Channels.pipeline();

            //pipeline.addLast("debug", new LoggingHandler());

            pipeline.addLast("frameDecoder", new Frame.Decoder(tracker, connectionFactory));
            pipeline.addLast("frameEncoder", frameEncoder);

            pipeline.addLast("frameDecompressor", frameDecompressor);
            pipeline.addLast("frameCompressor", frameCompressor);

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);

            pipeline.addLast("handler", responseHandler);

            return pipeline;
        }
    }

    private class SecurePipelineFactory extends PipelineFactory
    {
        private final SSLContext sslContext;

        public SecurePipelineFactory() throws IOException
        {
            this.sslContext = SSLFactory.createSSLContext(encryptionOptions, true);
        }

        public ChannelPipeline getPipeline() throws Exception
        {
            SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(true);
            sslEngine.setEnabledCipherSuites(encryptionOptions.cipher_suites);
            ChannelPipeline pipeline = super.getPipeline();

            pipeline.addFirst("ssl", new SslHandler(sslEngine));
            return pipeline;
        }
    }

    private static class ResponseHandler extends SimpleChannelUpstreamHandler
    {
        public final BlockingQueue<Message.Response> responses = new SynchronousQueue<Message.Response>(true);

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        {
            assert e.getMessage() instanceof Message.Response;
            logger.info(e.getMessage().getClass().getName());

            SaslAuthClient saslAuthClient = SaslAuthClient.SASL_NETTY_CLIENT.get(ctx.getChannel());

            if (e.getMessage() instanceof SaslCompleteMessage)
            {
                logger.info("Server has sent us the SaslComplete message. Allowing normal work to proceed.");
                if (!saslAuthClient.isComplete())
                {
                        String error = "Server returned a Sasl complete message, but client authentication is not complete";
                        logger.error(error);
                        throw new RuntimeException(error);
                }
            }

            // the message is not a SaslCompleteMessage, so check whether it contains a token from the server
            if (e.getMessage() instanceof SaslTokenResponseMessage)
            {
                SaslTokenResponseMessage serverToken = (SaslTokenResponseMessage) e.getMessage();
                // Generate SASL response, but we only actually send the response if it's non-null (null is a valid value if
                // auth is completed).
                byte[] responseToServer = saslAuthClient.evaluateServerToken(serverToken);
                if (responseToServer == null) {
                    // If we generate a null response, then authentication has completed (if
                    // not, warn), and return without sending a response back to the server.
                    logger.debug("Response to server is null: authentication should now be complete.");
                    if (!saslAuthClient.isComplete()) {
                        logger.warn("Generated a null response, but authentication is not complete.");
                    }
                    return;
                } else {
                    logger.debug("Response to server token has length: " + responseToServer.length);
                }
                // Construct a message containing the SASL response and send it to the server.
                logger.debug("Sending response message back to server");
                ctx.getChannel().write(new SaslTokenRequestMessage(responseToServer));
                return;
            }

            try
            {
                responses.put((Message.Response) e.getMessage());
            } catch (InterruptedException ie)
            {
                throw new RuntimeException(ie);
            }

        }

        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
        {
            if (this == ctx.getPipeline().getLast())
                logger.error("Exception in response", e.getCause());
            ctx.sendUpstream(e);
        }
    }


    private Subject getSubject()
    {
        Subject subject = new Subject();
        try
        {
            LoginContext login = new LoginContext("Client", subject, null, new KerberosUserConfiguration());
            login.login();
            return subject;
        }
        catch(LoginException e)
        {
            throw new RuntimeException(e);
        }
    }

    public class KerberosUserConfiguration extends Configuration
    {
        private final Map<String, String> kerberosOptions = Maps.newHashMap();

        {
            kerberosOptions.put("doNotPrompt", "true");
            kerberosOptions.put("renewTGT", "true");
            kerberosOptions.put("storeKey", "false");
            kerberosOptions.put("useKeyTab", "false");
            kerberosOptions.put("useTicketCache", "true");
            kerberosOptions.put("ticketCache", System.getenv("KRB5CCNAME"));

        }

        private final AppConfigurationEntry kerberosLogin =
                new AppConfigurationEntry(Krb5LoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                        kerberosOptions);

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String arg0)
        {
            return new AppConfigurationEntry[]{kerberosLogin};
        }
    }
}

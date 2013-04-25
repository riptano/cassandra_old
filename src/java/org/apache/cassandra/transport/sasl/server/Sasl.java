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
package org.apache.cassandra.transport.sasl.server;

import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.security.Provider;
import java.security.Security;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Sasl
{

    private static final Logger logger = LoggerFactory.getLogger(Sasl.class);

    public static final ChannelLocal<SaslAuthBridge> SASL_AUTH_BRIDGE = new ChannelLocal<SaslAuthBridge>();

    private static Map<Class<? extends IAuthenticator>, SaslAuthBridgeFactory> REGISTRY =
            new ConcurrentHashMap<Class<? extends IAuthenticator>, SaslAuthBridgeFactory>();

    public static void register(Class<? extends IAuthenticator> authenticatorClass, SaslAuthBridgeFactory factory)
    {
        REGISTRY.put(authenticatorClass, factory);
    }

    public static SaslAuthBridgeFactory deregister(Class<? extends IAuthenticator> authenticatorClass)
    {
        return REGISTRY.remove(authenticatorClass);
    }

    public static boolean isSaslBridgeRegistered()
    {
        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        if (authenticator instanceof AllowAllAuthenticator)
        {
            // no auth required
            return true;
        }
        return REGISTRY.containsKey(authenticator.getClass());
    }

    public static SaslAuthBridge getSaslAuthBridge(Channel channel) throws SaslException
    {
        // initialize server-side SASL functionality, if we haven't yet already
        // (in which case we are processing the first SASL message from the client).
        SaslAuthBridge saslAuthBridge = SASL_AUTH_BRIDGE.get(channel);
        if (saslAuthBridge == null)
        {
            logger.debug("No Sasl bridge found for " + channel + ", creating now");
            Class authenticatorClass = DatabaseDescriptor.getAuthenticator().getClass();
            // the registry should return a factory/provider, not an instance
            SaslAuthBridgeFactory factory = REGISTRY.get(authenticatorClass);
            // create a new instance using the factory
            saslAuthBridge = factory.newInstance();
            SASL_AUTH_BRIDGE.set(channel, saslAuthBridge);
        }
        else
        {
            logger.debug("Found existing Sasl bridge on server:" + channel.getLocalAddress()
                    + " for client " + channel.getRemoteAddress());
        }
        return saslAuthBridge;
    }

    public static class SaslProvider extends Provider
    {
        private SaslProvider()
        {
            super("CassandraSasl Provider", 1.0, "Provider of SASL factories for plain text login");
            this.put("SaslServerFactory.PLAIN", PlainTextSaslServer.Factory.class.getName());
        }

        private static final SaslProvider INSTANCE = new SaslProvider();

        public static SaslProvider instance()
        {
            return INSTANCE;
        }
    }

    static
    {
        Security.addProvider(SaslProvider.instance());
        logger.info("Added Cassandra SASL provider");
        register(PasswordAuthenticator.class, new PlainTextSaslAuthBridge.Factory());
        logger.info("Registered default SASL bridge implementations");
    }

}

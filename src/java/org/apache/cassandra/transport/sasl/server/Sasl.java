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

    private static Map<Class<? extends IAuthenticator>, SaslAuthBridgeFactory> REGISTRY =
            new ConcurrentHashMap<Class<? extends IAuthenticator>, SaslAuthBridgeFactory>();

    /**
     * Links an IAuthenticator implementation with a SaslAuthBridgeFactory.
     * When a custom IAuthenticator is added, the native server needs a means
     * to be able to extract the credentials information it requires from a
     * Sasl server implementation. The SaslAuthBridge interface provides this,
     * and the AbstractSaslAuthBridge class a base for new implementations.
     * @param authenticatorClass
     * @param factory
     */
    public static void register(Class<? extends IAuthenticator> authenticatorClass, SaslAuthBridgeFactory factory)
    {
        REGISTRY.put(authenticatorClass, factory);
    }

    public static SaslAuthBridgeFactory deregister(Class<? extends IAuthenticator> authenticatorClass)
    {
        return REGISTRY.remove(authenticatorClass);
    }

    public static SaslAuthBridge newSaslBridge(Class<? extends IAuthenticator> authenticatorClass)
    throws SaslException
    {
        SaslAuthBridgeFactory factory = REGISTRY.get(authenticatorClass);
        return factory.newInstance();
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

    /**
     * A Sasl provider which can be used to create Sasl server instances
     * that support the PLAIN text mechanism
     */
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

    /**
     * Register custom SASL Provider with Java security & hook up the
     * standard PasswordAuthenticator with the Sasl auth bridge which
     * supports the PLAIN text mechanism
     */
    static
    {
        Security.addProvider(SaslProvider.instance());
        logger.info("Added Cassandra SASL provider");
        register(PasswordAuthenticator.class, new PlainTextSaslAuthBridge.Factory());
        logger.info("Registered default SASL bridge implementations");
    }

}

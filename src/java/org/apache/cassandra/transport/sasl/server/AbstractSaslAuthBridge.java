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

import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Map;

public abstract class AbstractSaslAuthBridge implements SaslAuthBridge
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractSaslAuthBridge.class);

    public static final String SASL_PROTOCOL_NAME = "cassandra";
    public static final CallbackHandler NULL_CBH = new CallbackHandler()
    {
        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
        {
        }
    };

    private final Subject serverIdentity;
    protected final SaslServer saslServer;

    public AbstractSaslAuthBridge()
    {
        try
        {
            this.serverIdentity = getServerIdentity();
        } catch (LoginException e)
        {
            logger.error("Error obtaining subject for server identity", e);
            throw new RuntimeException(e);
        }

        saslServer = Subject.doAs(serverIdentity, new PrivilegedAction<SaslServer>()
        {
            @Override
            public SaslServer run()
            {
                try
                {
                    return Sasl.createSaslServer(getMechanism(),
                            getProtocol(),
                            getHostname(),
                            getProperties(),
                            getCallbackHandler());
                } catch (SaslException e)
                {
                    logger.error("Error initialising SASL server", e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public Subject getServerIdentity() throws LoginException
    {
        Subject subject = new Subject();
        final String userName = System.getProperty("user.name");
        subject.getPrincipals().add(new Principal()
        {
            @Override
            public String getName()
            {
                return userName;
            }
        });
        return subject;
    }

    public String getProtocol()
    {
        return SASL_PROTOCOL_NAME;
    }

    public String getHostname()
    {
        return FBUtilities.getBroadcastAddress().getHostName();
    }

    public Map<String, String> getProperties()
    {
        return Collections.emptyMap();
    }

    public CallbackHandler getCallbackHandler()
    {
        return NULL_CBH;
    }

    public boolean isComplete()
    {
        return saslServer.isComplete();
    }

    /**
     * Used by SaslTokenRequestMessage.processToken() to respond to client SASL tokens.
     *
     * @param token Server's SASL token
     * @return token to send back to the client.
     */
    public byte[] evaluateResponse(final byte[] token)
    {
        return Subject.doAs(serverIdentity, new PrivilegedAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                try
                {
                    logger.debug("Evaluating input token");
                    byte[] retval = saslServer.evaluateResponse(token);
                    return retval;
                } catch (SaslException e)
                {
                    logger.error("Failed to evaluate client token", e);
                    return null;
                }
            }
        });
    }
}

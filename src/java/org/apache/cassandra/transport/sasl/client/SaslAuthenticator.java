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
package org.apache.cassandra.transport.sasl.client;

import org.apache.cassandra.transport.messages.SaslTokenRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper around {@code SaslClient}, which performs necessary parts of
 * the authentication protocol as {@code PrivilegedAction}s with the
 * assumed identity of the {@code Subject} supplied in the constructor.
 */
public class SaslAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(SaslAuthenticator.class);

    public static final Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>()
    {{
            put(Sasl.SERVER_AUTH, "true");
            put(Sasl.QOP, "auth");
    }};

    /**
     * Used to respond to server challenges with SASL tokens
     * represented as byte arrays.
     */
    private final SaslClient saslClient;

    /**
     * The identity we intend to use when we authenticate with the server
     */
    private Subject clientIdentity;

    public SaslAuthenticator(final Subject clientIdentity,
                             final String[] mechanisms,
                             final String authzId,
                             final String protocol,
                             final String hostname,
                             final Map<String, String> properties,
                             final CallbackHandler callbackHandler)
    {
        logger.trace("Initalising SASL client");
        this.clientIdentity = clientIdentity;
        saslClient = Subject.doAs(this.clientIdentity, new PrivilegedAction<SaslClient>()
        {
            @Override
            public javax.security.sasl.SaslClient run()
            {
                try
                {
                    return Sasl.createSaslClient(
                            mechanisms,
                            authzId,
                            protocol,
                            hostname,
                            properties,
                            callbackHandler);
                } catch (Exception e)
                {
                    logger.error("Error initialising SASL client", e);
                    throw new RuntimeException(e);
                }
            }
        });
        logger.trace("SASL client initialised");
    }


    /**
     * Generate an initial token to start the SASL handshake with server.
     * @return SaslTokenRequestMessage message to be sent to server.
     * @throws java.io.IOException
     */
    public SaslTokenRequestMessage firstToken()
    {
        logger.trace("Initialising SASL handshake from client");
        byte[] saslToken = null;
        if (saslClient.hasInitialResponse())
        {
            saslToken = Subject.doAs(clientIdentity, new PrivilegedAction<byte[]>()
            {
                @Override
                public byte[] run()
                {
                    try
                    {
                        byte[] saslToken = new byte[0];
                        return saslClient.evaluateChallenge(saslToken);
                    } catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        SaslTokenRequestMessage saslTokenMessage = new SaslTokenRequestMessage(saslToken);
        return saslTokenMessage;
    }

    /**
     * Respond to server's SASL token
     * @param saslTokenMessage contains server's SASL token
     * @return client's response SASL token, which will be null if authentication is complete
     */
    public byte[] evaluateServerToken(final byte[] serverToken)
    {
        logger.trace("Evaluating SASL server token");
        if (saslClient.isComplete())
        {
            return null;
        }
        byte[] retval = Subject.doAs(clientIdentity, new PrivilegedAction<byte[]>()
        {
            @Override
            public byte[] run()
            {
                try
                {

                    logger.trace("SASL client evaluating challenge");
                    byte[] retval = saslClient.evaluateChallenge(serverToken);
                    logger.trace("SASL client is complete = " + saslClient.isComplete());
                    return retval;
                }
                catch (SaslException e)
                {
                    logger.error("Failed to generate response to SASL server's token:", e);
                    throw new RuntimeException(e);
                }
            }
        });
        return retval;
    }

    /**
     * Is the SASL negotiation with the server complete
     * @return true iff SASL authentication has successfully completed
     */
    public boolean isComplete()
    {
        return saslClient.isComplete();
    }
}

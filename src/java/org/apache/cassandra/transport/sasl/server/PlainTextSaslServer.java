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

import org.apache.cassandra.auth.IAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * SaslServer implementation that supports the PLAIN mechanism &
 * also makes the decoded credentials accessible. These are used
 * by PlainTextSaslAuthBridge to supply the Map<String, String>
 * required by PasswordAuthenticator (and potentially other
 * IAuthenticator implementations).
 */
public class PlainTextSaslServer extends AbstractSaslServer
{
    private static final Logger logger = LoggerFactory.getLogger(PlainTextSaslServer.class);

    public static final String PLAIN_MECHANISM = "PLAIN";

    private static final byte NUL = 0;
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    private Map<String, String> credentials;

    public PlainTextSaslServer(final String protocol, final String serverName, final CallbackHandler callbackHandler)
    {
        super(PLAIN_MECHANISM, protocol, serverName, callbackHandler);
        setState(SaslState.INITIAL);
        logger.debug("Initialised new PlainTextSaslServer instance");
    }

    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException
    {
        credentials = decodeCredentials(response);
        setState(SaslState.COMPLETE);
        return null;
    }

    /**
     * SASL PLAIN mechanism specifies that credentials are encoded in a
     * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
     * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}
     * authzId is optional, and in fact we don't care about it here as we'll
     * set the authzId to match the authnId (that is, there is no concept of
     * a user being authorized to act on behalf of another).
     *
     * @param bytes encoded credentials string sent by the client
     * @return map containing the username/password pairs in the form an IAuthenticator
     * would expect
     * @throws SaslException
     */
    private Map<String, String> decodeCredentials(byte[] bytes) throws SaslException
    {
        logger.debug("Decoding credentials from client token");
        byte[] user = null;
        byte[] pass = null;
        int end = bytes.length;
        for (int i = bytes.length - 1 ; i >= 0; i--)
        {
            if (bytes[i] == NUL)
            {
                if (pass == null)
                    pass = Arrays.copyOfRange(bytes, i + 1, end);
                else if (user == null)
                    user = Arrays.copyOfRange(bytes, i + 1, end);
                end = i;
            }
        }

        if (user == null)
        {
            throw new SaslException("Authentication ID must not be null");
        }
        if (pass == null)
        {
            throw new SaslException("Password must not be null");
        }

        Map<String, String> credentials = new HashMap<String, String>();
        credentials.put(IAuthenticator.USERNAME_KEY, new String(user, UTF8_CHARSET));
        credentials.put(IAuthenticator.PASSWORD_KEY, new String(pass, UTF8_CHARSET));
        return credentials;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException
    {
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException
    {
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public String getAuthorizationID()
    {
        if(isComplete())
            return credentials.get(IAuthenticator.USERNAME_KEY);
        else
            throw new IllegalStateException("PLAIN authentication not completed");
    }

    public Map<String, String> getCredentials()
    {
        if(isComplete())
            return credentials;
        else
            throw new IllegalStateException("PLAIN authentication not completed");
    }

    public static class Factory implements SaslServerFactory
    {

        @Override
        public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props,
                                           CallbackHandler cbh) throws SaslException
        {
            return new PlainTextSaslServer(protocol, serverName, cbh);
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props)
        {
            return new String[] {PLAIN_MECHANISM};
        }
    }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

public abstract class AbstractSaslServer implements SaslServer
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractSaslServer.class);

    public enum SaslState
    {
        INITIAL, COMPLETE, FAILED;
    }

    private final String mechanism;
    private final String protocol;
    private final String serverName;
    private final CallbackHandler callbackHandler;

    private SaslState state;

    public AbstractSaslServer(String mechanism, String protocol, String serverName, CallbackHandler callbackHandler)
    {
        this.mechanism = mechanism;
        this.protocol = protocol;
        this.serverName = serverName;
        this.callbackHandler = callbackHandler;
        this.state = SaslState.INITIAL;
    }

    public String getMechanismName()
    {
        return mechanism;
    }

    public boolean isComplete()
    {
        return state == SaslState.COMPLETE;
    }

    public Object getNegotiatedProperty(String propName)
    {
        return null;
    }

    public void dispose() throws SaslException
    {
    }

    public String getProtocol()
    {
        return protocol;
    }

    public String getServerName()
    {
        return serverName;
    }

    protected void setState(SaslState state)
    {
        this.state = state;
    }

    protected SaslState getState()
    {
        return state;
    }
}

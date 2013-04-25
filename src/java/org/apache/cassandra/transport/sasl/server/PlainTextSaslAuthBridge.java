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

import javax.security.sasl.SaslException;

import java.util.Map;

public class PlainTextSaslAuthBridge extends AbstractSaslAuthBridge
{
    private static final Logger logger = LoggerFactory.getLogger(PlainTextSaslAuthBridge.class);

    @Override
    public String getMechanism()
    {
        return PlainTextSaslServer.PLAIN_MECHANISM;
    }

    @Override
    public Map<String, String> getCredentials() throws SaslException
    {
        logger.debug("Getting credentials from SaslServer: " + saslServer.getClass().getName());
        if (saslServer instanceof PlainTextSaslServer)
            return ((PlainTextSaslServer)saslServer).getCredentials();
        else
            throw new SaslException("Incompatible SaslServer implementation. " +
                    "Expected PlainTextSaslServer, received " + saslServer.getClass().getName());
    }

    public static class Factory implements SaslAuthBridgeFactory
    {
        @Override
        public SaslAuthBridge newInstance() throws SaslException
        {
            return new PlainTextSaslAuthBridge();
        }
    }

}

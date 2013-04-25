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
package org.apache.cassandra.transport.messages;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.sasl.server.Sasl;
import org.apache.cassandra.transport.sasl.server.SaslAuthBridge;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.util.Map;

public class SaslTokenRequestMessage extends Message.Request
{
    private static final Logger logger = LoggerFactory.getLogger(SaslTokenRequestMessage.class);

    public static final Message.Codec<SaslTokenRequestMessage> codec = new Message.Codec<SaslTokenRequestMessage>()
    {
        @Override
        public SaslTokenRequestMessage decode(ChannelBuffer body, int version)
        {
            return new SaslTokenRequestMessage(CBUtil.readBytes(body));
        }

        @Override
        public ChannelBuffer encode(SaslTokenRequestMessage saslTokenMessage)
        {
            return CBUtil.bytesToCB(saslTokenMessage.getSaslToken());
        }
    };

    private byte[] token;

    public SaslTokenRequestMessage(byte[] token)
    {
        super(Message.Type.SASL_REQUEST);
        this.token = token;
    }

    public byte[] getSaslToken()
    {
        return token;
    }

    @Override
    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    @Override
    public Response execute(QueryState queryState)
    {
        logger.debug("Executing sasl token message request");
        try
        {
            SaslAuthBridge saslBridge = Sasl.getSaslAuthBridge(connection.channel());
            byte[] updatedToken = saslBridge.evaluateResponse(getSaslToken());
            if (saslBridge.isComplete())
            {
                Map<String, String> credentials = saslBridge.getCredentials();
                try
                {
                    queryState.getClientState().login(credentials);
                } catch (AuthenticationException e)
                {
                    return ErrorMessage.fromException(e);
                }

                // If authentication of client is complete, we will also send a completed message to the client
                logger.debug("SASL authentication is complete, sending sasl complete message back to client");
                return new SaslCompleteMessage();
            }
            else
            {
                logger.debug("Sending updated token message back downstream to client");
                return new SaslTokenResponseMessage(updatedToken);
            }
        }
        catch(SaslException e)
        {
            return ErrorMessage.fromException(e);
        }
    }
}

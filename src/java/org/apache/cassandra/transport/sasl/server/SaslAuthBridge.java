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

import javax.security.sasl.SaslException;
import java.util.Map;

/**
 * IAuthenticator.login() requires a Map<String, String> containing
 * client credentials. How those credentials are used to perform the
 * authentication is an implementation detail. CQL native protocol server
 * supports SASL authentication and this interface provides the bridge
 * between the two schemes. A SaslAuthBridge implementation should wrap
 * a SaslServer and be able to extract the credentials info required by
 * the IAuthenticator from the SaslServer.
 *
 * see PlainTextSaslAuthBridge for an example; this wraps a custom
 * SaslServer implementation which supports the SASL PLAIN mechanism.
 * During the SASL negotiation, client credentials are transmitted in
 * NUL delimted, plain text byte array. PlainTextSaslServer decodes these
 * and makes them available in the required Map form.
 * PlainTextSaslAuthBridge simply wraps the sasl server to provide an
 * accessor.
 *
 * When a custom IAuthenticator is added, the native server needs a
 * compatible SaslAuthBridge implementation to work with. In any case
 * where the client actually sends the credentials in the standard
 * Map<String, String> form (via thrift login() or the native Credentials
 * message) PlainTextSaslAuthBridge can be used. Where the credentials
 * transmission is done via alternative means, it will be necessary to
 * also provide a SaslAuthBridge implementation.
 *
 * CQL native server uses the utility class Sasl to lookup which auth
 * bridge implementation to use for the for the configured IAuthenticator.
 * Custom IAuthenticator implementations need to register their
 * counterpart SaslAuthBridge implementation (or rather a factory for
 * creating instances) using Sasl's register method. It is recommended
 * to do this in a static initialisation block in the IAuthenticator class.
 *
 */
public interface SaslAuthBridge
{
    public String getMechanism();
    public byte[] evaluateResponse(byte[] token) throws SaslException;
    public boolean isComplete();
    public Map<String, String> getCredentials() throws SaslException;
}

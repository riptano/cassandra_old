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

package org.apache.cassandra.transport.sasl.server.krb;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.transport.sasl.server.Sasl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * TEMPORARILY HACKED IN FROM DSE
 * Real authentication be performed by the KDC server.
 */
public class KerberosAuthenticator implements IAuthenticator
{
    static {
        Sasl.register(KerberosAuthenticator.class, new KerberosSaslAuthBridge.Factory());
    }

    public boolean requireAuthentication()
    {
        return true;
    }

    public Set<Option> supportedOptions()
    {
        return Collections.emptySet();
    }

    public Set<Option> alterableOptions()
    {
        return Collections.emptySet();
    }

    public AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException
    {
        String user = credentials.get(USERNAME_KEY);

        if (user == null)
            throw new AuthenticationException("Authentication request was missing the required key '" + USERNAME_KEY + "'");

        String names[] = user.split("[/@]");

        // we use the short name without host name for the default cassandra super user
        if (Auth.DEFAULT_SUPERUSER_NAME.equals(names[0]))
            return new AuthenticatedUser(Auth.DEFAULT_SUPERUSER_NAME);
        else
            return new AuthenticatedUser(user);
    }

    public void create(String username, Map<Option, Object> options) throws RequestValidationException, RequestExecutionException
    {
    }

    public void alter(String username, Map<Option, Object> options) throws RequestValidationException, RequestExecutionException
    {
    }

    public void drop(String username) throws RequestValidationException, RequestExecutionException
    {
    }

    public Set<IResource> protectedResources()
    {
        return Collections.emptySet();
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
    }

    public void setupDefaultUser()
    {
    }

}

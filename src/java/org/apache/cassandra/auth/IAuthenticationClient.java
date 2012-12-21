package org.apache.cassandra.auth;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.thrift.AuthenticationException;

/**
 * <p>A client to be used to connect to security server to authenticate the user</p>
 *
 * <p>
 * There are some Cassandra tools, e.g. SSTable related commands which are used off line. They need be authenticated 
 * to access the related resources. We can use some external authentication server, e.g. Kerberos server or 
 * LDAP security server. By connecting this client to the security server, we can do the authentication check.
 * </p>
 * 
 * <p>
 * connect method doesn't return anything. It throws @AuthenticationException, which it should be caught at client code as authentication failures.
 * For the connection session to the server, we need make sure it's short life and it needs be recycled in time to prevent resource leakage. 
 * </p>
 */
public interface IAuthenticationClient
{
    /**
     * Connect to the security server. It throws @AuthenticationException if the connection fails, so you should catch this at your client side code
     * @throws @AuthenticationException, you can wrap other specific exception as an authentication exception. e.g. LoginException, PrivilegedActionException
     */
    public void connect() throws AuthenticationException;
    
    /**
     * Validate the configuration
     * @throws ConfigurationException
     */
    public void validateConfiguration() throws ConfigurationException;
}

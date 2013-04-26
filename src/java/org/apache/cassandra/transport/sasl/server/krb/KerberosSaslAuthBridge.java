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

import com.sun.security.auth.module.Krb5LoginModule;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.transport.sasl.server.*;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * BELONGS IN DSE
 */
public class KerberosSaslAuthBridge extends AbstractSaslAuthBridge
{

    private static final Logger logger = LoggerFactory.getLogger(KerberosSaslAuthBridge.class);

    public static final String GSSAPI_MECHANISM = "GSSAPI";
    public static final String DSE_KRB5_PROTOCOL_NAME = "dse";

    @Override
    public String getMechanism()
    {
        return GSSAPI_MECHANISM;
    }

    public Subject getServerIdentity() throws LoginException
    {
        // normally this would be read from DSE config
        String principal = System.getProperty("krb.principal");
        String keytab = System.getProperty("krb.keytab");

        Subject subject = new Subject();
        LoginContext loginCtx = new LoginContext(principal, subject, null, new KrbConfiguration(principal, keytab));
        loginCtx.login();
        return subject;
    }

    public String getProtocol()
    {
        return DSE_KRB5_PROTOCOL_NAME;
    }

    public String getHostname()
    {
        return FBUtilities.getBroadcastAddress().getHostName();
    }

    @Override
    public CallbackHandler getCallbackHandler()
    {
        return new CallbackHandler()
        {
            @Override
            public void handle(Callback[] callbacks) throws
            UnsupportedCallbackException
            {
                AuthorizeCallback ac = null;
                for (Callback callback : callbacks)
                {
                    if (callback instanceof AuthorizeCallback)
                    {
                        ac = (AuthorizeCallback) callback;
                    }
                    else
                    {
                        throw new UnsupportedCallbackException(callback,
                                "Unrecognized SASL GSSAPI Callback");
                    }
                }
                if (ac != null)
                {
                    String authid = ac.getAuthenticationID();
                    String authzid = ac.getAuthorizationID();
                    if (authid.equals(authzid))
                    {
                        ac.setAuthorized(true);
                    }
                    else
                    {
                        ac.setAuthorized(false);
                    }
                    if (ac.isAuthorized())
                    {
                        logger.debug("SASL server GSSAPI callback: setting "
                                + "canonicalized client ID: " + authzid);
                        ac.setAuthorizedID(authzid);
                    }
                }
            }
        };
    }

    @Override
    public Map<String, String> getCredentials() throws SaslException
    {
        return Collections.singletonMap(IAuthenticator.USERNAME_KEY, saslServer.getAuthorizationID());
    }

    private static class KrbConfiguration extends javax.security.auth.login.Configuration
    {

        private final Map<String, String> keytabKerberosOptions =
                new HashMap<String, String>();

        KrbConfiguration(String principal, String keytab)
        {
            keytabKerberosOptions.put("doNotPrompt", "true");
            keytabKerberosOptions.put("useKeyTab", "true");
            keytabKerberosOptions.put("storeKey", "true");
            keytabKerberosOptions.put("keyTab", keytab);
            keytabKerberosOptions.put("principal", principal);
        }

        private final AppConfigurationEntry keytabKerberosLogin =
                new AppConfigurationEntry(Krb5LoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        keytabKerberosOptions);

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String arg0)
        {
            return new AppConfigurationEntry[]{keytabKerberosLogin};
        }
    }

    public static class Factory implements SaslAuthBridgeFactory
    {

        @Override
        public SaslAuthBridge newInstance() throws SaslException
        {
            return new KerberosSaslAuthBridge();
        }
    }

    static
    {
        Sasl.register(KerberosAuthenticator.class, new KerberosSaslAuthBridge.Factory());
        logger.info("Registered Kerberos SASL bridge implementation");
    }

}

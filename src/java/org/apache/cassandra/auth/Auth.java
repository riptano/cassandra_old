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
package org.apache.cassandra.auth;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DefsTable;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;

public class Auth
{
    private static final Logger logger = LoggerFactory.getLogger(Auth.class);

    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";

    private static final long SUPERUSER_SETUP_DELAY = 10; // seconds

    // 'system_auth' in 1.2.
    public static final String AUTH_KS = "dse_auth";
    public static final String USERS_CF = "users";

    private static final String USERS_CF_SCHEMA =
        String.format("CREATE TABLE %s.%s (name text PRIMARY KEY, super boolean) WITH gc_grace_seconds=864000",
                      AUTH_KS,
                      USERS_CF);

    /**
     * Checks if the username is stored in AUTH_KS.USERS_CF.
     *
     * @param username Username to query.
     * @return whether or not Cassandra knows about the user.
     */
    public static boolean isExistingUser(String username)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        String query = String.format("SELECT * FROM %s.%s USING CONSISTENCY %s WHERE name = '%s'",
                                     AUTH_KS,
                                     USERS_CF,
                                     consistencyForUser(username),
                                     escape(username));
        return !QueryProcessor.processInternal(query).type.equals(CqlResultType.VOID);
    }

    /**
     * Checks if the user is a known superuser.
     *
     * @param username Username to query.
     * @return true is the user is a superuser, false if they aren't or don't exist at all.
     */
    public static boolean isSuperuser(String username)
    {
        String query = String.format("SELECT super FROM %s.%s USING CONSISTENCY %s WHERE name = '%s'",
                                     AUTH_KS,
                                     USERS_CF,
                                     consistencyForUser(username),
                                     escape(username));
        try
        {
            CqlResult result = QueryProcessor.processInternal(query);
            return !result.type.equals(CqlResultType.VOID) && new UntypedResultSet(result.rows).one().getBoolean("super");
        }
        catch (Throwable e)
        {
            logger.error("Superuser check failed for user {}: {}", username, e.toString());
            return false;
        }
    }

    /**
     * Inserts the user into AUTH_KS.USERS_CF (or overwrites their superuser status as a result of an ALTER USER query).
     *
     * @param username Username to insert.
     * @param isSuper User's new status.
     */
    public static void insertUser(String username, boolean isSuper)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        QueryProcessor.processInternal(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', '%s') USING CONSISTENCY %s",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     escape(username),
                                                     isSuper,
                                                     consistencyForUser(username)));
    }

    /**
     * Deletes the user from AUTH_KS.USERS_CF.
     *
     * @param username Username to delete.
     */
    public static void deleteUser(String username)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        QueryProcessor.processInternal(String.format("DELETE FROM %s.%s USING CONSISTENCY %s WHERE name = '%s'",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     consistencyForUser(username),
                                                     escape(username)));
    }

    /**
     * Sets up dse_auth keyspace and dse_auth.users cf, also authenticator and authorizer ks/cfs if required.
     */
    public static void setup()
    {
        setupAuthKeyspace();
        setupUsersTable();
        authenticator().setup();
        authorizer().setup();
    }

    /**
     * Sets up default superuser ('cassandra') and calls authenticator's setupDefaultUser method.
     */
    public static void setupDefaultUsers()
    {
        if (DatabaseDescriptor.getSeeds().contains(FBUtilities.getBroadcastAddress()) || !DatabaseDescriptor.isAutoBootstrap())
        {
            setupDefaultSuperuser();
            authenticator().setupDefaultUser();
        }
    }

    // Only use QUORUM cl for the default superuser.
    private static String consistencyForUser(String username)
    {
        return username.equals(DEFAULT_SUPERUSER_NAME) ? "QUORUM" : "ONE";
    }

    // Create auth keyspace unless it's already been loaded.
    private static void setupAuthKeyspace()
    {
        Schema.instance.load(KSMetaData.newKeyspace(AUTH_KS,
                                                    DatabaseDescriptor.getAuthReplicationStrategy(),
                                                    DatabaseDescriptor.getAuthReplicationOptions(),
                                                    true,
                                                    Collections.EMPTY_LIST));
    }

    // Create users table unless it's already been loaded.
    private static void setupUsersTable()
    {
        DefsTable.addColumnFamily(CFMetaData.fromCQL3Schema(101, USERS_CF_SCHEMA));
    }

    /**
     * Sets up default superuser.
     */
    private static void setupDefaultSuperuser()
    {
        Runnable setup = new Runnable()
        {
            public void run()
            {
                try
                {
                    // insert a default superuser if AUTH_KS.USERS_CF is empty.
                    if (QueryProcessor.processInternal(String.format("SELECT * FROM %s.%s USING CONSISTENCY QUORUM",
                                                                     AUTH_KS,
                                                                     USERS_CF))
                                      .type.equals(CqlResultType.VOID))
                    {
                        String query = "INSERT INTO %s.%s (name, super) VALUES ('%s', '%s') USING CONSISTENCY QUORUM AND TIMESTAMP 0";
                        QueryProcessor.processInternal(String.format(query, AUTH_KS, USERS_CF, DEFAULT_SUPERUSER_NAME, true));
                        logger.info("Created default superuser {}", DEFAULT_SUPERUSER_NAME);
                    }
                }
                catch (Throwable e) // unavaiable or timedout
                {
                    logger.warn("Skipped creating default superuser: {}", e.toString());
                }
            }
        };

        StorageService.tasks.schedule(setup, SUPERUSER_SETUP_DELAY, TimeUnit.SECONDS);
    }

    // we only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static IAuthenticator authenticator()
    {
        return DatabaseDescriptor.getAuthenticator();
    }

    private static IAuthorizer authorizer()
    {
        return DatabaseDescriptor.getAuthorizer();
    }
}

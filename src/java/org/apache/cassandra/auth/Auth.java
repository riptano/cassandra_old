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

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.*;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;

public class Auth
{
    private static final Logger logger = LoggerFactory.getLogger(Auth.class);

    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";

    // 'system_auth' in 1.2.
    public static final String AUTH_KS = "dse_auth";
    public static final String USERS_CF = "users";

    private static final String AUTH_KS_SCHEMA =
        String.format("CREATE KEYSPACE %s WITH strategy_class = 'SimpleStrategy' AND strategy_options:replication_factor = 1",
                      AUTH_KS);

    private static final String USERS_CF_SCHEMA =
        String.format("CREATE TABLE %s.%s (name text PRIMARY KEY, super boolean) WITH gc_grace_seconds=864000",
                      AUTH_KS, USERS_CF);

    /**
     * Checks if the username is stored in AUTH_KS.USERS_CF.
     *
     * @param username Username to query.
     * @return whether or not Cassandra knows about the user.
     */
    public static boolean isExistingUser(String username)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        String query = String.format("SELECT * FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, escape(username));
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
        String query = String.format("SELECT super FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, escape(username));
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
        QueryProcessor.processInternal(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', '%s')",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     escape(username),
                                                     isSuper));
    }

    /**
     * Deletes the user from AUTH_KS.USERS_CF.
     *
     * @param username Username to delete.
     */
    public static void deleteUser(String username)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        QueryProcessor.processInternal(String.format("DELETE FROM %s.%s WHERE name = '%s'",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     escape(username)));
    }

    /**
     * Sets up dse_auth keyspace and dse_auth.users cf, also authenticator and authorizer ks/cfs if required.
     */
    public static void setup()
    {
        // ALL THE HACKS BELOW WILL BE REMOVED ONCE DSP-1379 IS REVERTED

        // Issue: if a node is a seed (or is already bootstrapped), then there is no delay to see the peers.
        // A fresh new node starts up, sees that it's the only node in the ring and it doesn't yet have dse_auth keyspace.
        // But by the time that keyspace creation gets to validating schema agreement, its peers are up, and schema versions don't
        // match, and an SDE is thrown since validateSchemaAgreement() fails.
        // Solution: add a small delay so that even if the node is a seed, it'll still wait a little for its peers to show
        // up and sync schema.
        // Another issue is default superuser setup. We have to check if any users exist, and if not - insert one
        // ('cassandra'). For this we also need peers to show up.
        logger.info("Waiting 5 seconds for peers to show up before setting up auth");
        try
        {
            TimeUnit.MILLISECONDS.sleep(5000);
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }

        // A temporary hack to reduce the possibility of SchemaDisagreementException during auth keyspace and cfs
        // creation. Not bullet-proof, but arguably Good Enough For Now.
        if (isSchemaCreatorNode())
        {

            if (!isSchemaCreated())
            {
                logger.info("Creating auth schema");
                setupAuthKeyspace();
                setupUsersTable();
                logger.info("Done creating auth schema");
            }

            authenticator().setup();
            authorizer().setup();
        }
        else
        {
            // avoid excessive log records.
            if (isSchemaCreated())
                return;

            logger.info("Waiting for auth schema creation");
            long deadline = System.currentTimeMillis() + StorageService.RING_DELAY;
            while (System.currentTimeMillis() < deadline)
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e); // not supposed to happen.
                }

                if (isSchemaCreated())
                {
                    logger.info("Auth schema created");
                    return;
                }
            }

            throw new RuntimeException("Auth setup failed");
        }

        setupDefaultSuperuser();
    }

    // Create auth keyspace unless it's already been loaded.
    private static void setupAuthKeyspace()
    {
        if (Schema.instance.getKSMetaData(AUTH_KS) != null)
            return;

        try
        {
            QueryProcessor.processInternal(AUTH_KS_SCHEMA);
        }
        catch (Throwable e)
        {
            Throwables.propagate(e);
        }
    }

    // Create users table unless it's already been loaded.
    private static void setupUsersTable()
    {
        if (Schema.instance.getCFMetaData(AUTH_KS, USERS_CF) != null)
            return;

        try
        {
            QueryProcessor.processInternal(USERS_CF_SCHEMA);
        }
        catch (Throwable e)
        {
            Throwables.propagate(e);
        }
    }

    /**
     * Sets up default superuser.
     */
    private static void setupDefaultSuperuser()
    {
        try
        {
            // insert a default superuser if AUTH_KS.USERS_CF is empty.
            if (QueryProcessor.processInternal(String.format("SELECT * FROM %s.%s", AUTH_KS, USERS_CF)).type.equals(CqlResultType.VOID))
            {
                insertUser(DEFAULT_SUPERUSER_NAME, true);
                logger.info("Created default superuser {}", DEFAULT_SUPERUSER_NAME);
            }
        }
        catch (Throwable e)
        {
            logger.warn("Skipping default superuser setup: {}", e.getMessage());
        }
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

    private static boolean isSchemaCreatorNode()
    {
        List<InetAddress> candidates = new ArrayList<InetAddress>(Sets.intersection(Gossiper.instance.getLiveMembers(),
                                                                                    DatabaseDescriptor.getSeeds()));

        Collections.sort(candidates, new Comparator<InetAddress>(){
            public int compare(InetAddress a, InetAddress b)
            {
                return a.getHostAddress().compareTo(b.getHostAddress());
            }
        });

        return !candidates.isEmpty() && candidates.get(0).equals(FBUtilities.getBroadcastAddress());
    }

    private static boolean isSchemaCreated()
    {
        return Schema.instance.getCFMetaData(AUTH_KS, USERS_CF) != null;
    }
}

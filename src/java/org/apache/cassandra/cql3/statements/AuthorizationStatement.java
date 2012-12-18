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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;

public abstract class AuthorizationStatement extends ParsedStatement implements CQLStatement
{
    @Override
    public Prepared prepare()
    {
        return new Prepared(this);
    }

    public int getBoundsTerms()
    {
        return 0;
    }

    protected boolean isExistingUser(String username) throws InvalidRequestException
    {
        try
        {
            return Auth.isExistingUser(username);
        }
        catch (Exception e)
        {
            throw new InvalidRequestException(e.toString());
        }
    }

    public abstract CqlResult execute(ClientState state, List<ByteBuffer> variables) throws InvalidRequestException;

    public static DataResource maybeCorrectResource(DataResource resource, ClientState state) throws InvalidRequestException
    {
        if (resource.isColumnFamilyLevel() && resource.getKeyspace() == null)
            return DataResource.columnFamily(state.getKeyspace(), resource.getColumnFamily());
        return resource;
    }
}

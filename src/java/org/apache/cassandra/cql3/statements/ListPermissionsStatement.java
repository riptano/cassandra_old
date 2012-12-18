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
import java.util.*;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.*;

public class ListPermissionsStatement extends AuthorizationStatement
{
    private static final String USERNAME = "username";
    private static final String RESOURCE = "resource";
    private static final String PERMISSION = "permission";

    private final Set<Permission> permissions;
    private DataResource resource;
    private final String username;
    private final boolean recursive;

    public ListPermissionsStatement(Set<Permission> permissions, IResource resource, String username, boolean recursive)
    {
        this.permissions = permissions;
        this.resource = (DataResource) resource;
        this.username = username;
        this.recursive = recursive;
    }

    // TODO: user existence check (when IAuthenticator rewrite is done)
    public void validate(ClientState state) throws InvalidRequestException
    {
        // a check to ensure the existence of the user isn't being leaked by user existence check.
        if (username != null && !isExistingUser(username))
            throw new InvalidRequestException(String.format("User %s doesn't exist", username));

        if (resource != null)
        {
            resource = maybeCorrectResource(resource, state);
            if (!resource.exists())
                throw new InvalidRequestException(String.format("%s doesn't exist", resource));
        }
    }

    public void checkAccess(ClientState state) throws InvalidRequestException
    {
        state.ensureNotAnonymous();
    }

    public CqlResult execute(ClientState state, List<ByteBuffer> variables) throws InvalidRequestException
    {
        List<PermissionDetails> details = new ArrayList<PermissionDetails>();

        if (resource != null && recursive)
        {
            for (IResource r : Resources.chain(resource))
                details.addAll(list(state, r));
        }
        else
        {
            details.addAll(list(state, resource));
        }

        Collections.sort(details);
        return cqlResult(details);
    }

    private CqlResult cqlResult(List<PermissionDetails> details)
    {
        if (details.isEmpty())
            return null;

        List<CqlRow> rows = new ArrayList<CqlRow>();
        for (PermissionDetails pd : details)
        {
            List<Column> columns = new ArrayList<Column>(3);
            columns.add(createColumn(USERNAME, pd.username));
            columns.add(createColumn(RESOURCE, pd.resource.toString()));
            columns.add(createColumn(PERMISSION, pd.permission.toString()));
            rows.add(new CqlRow(UTF8Type.instance.decompose(""), columns));
        }

        CqlResult result = new CqlResult();
        result.type = CqlResultType.ROWS;
        result.schema = new CqlMetadata(Collections.<ByteBuffer, String>emptyMap(),
                                        Collections.<ByteBuffer, String>emptyMap(),
                                        "UTF8Type",
                                        "UTF8Type");
        result.rows = rows;
        return result;
    }

    private Column createColumn(String name, String value)
    {
        return new Column(UTF8Type.instance.decompose(name)).setValue(UTF8Type.instance.decompose(value));
    }

    private Set<PermissionDetails> list(ClientState state, IResource resource) throws UnauthorizedException, InvalidRequestException
    {
        return DatabaseDescriptor.getAuthorizer().list(state.getUser(), permissions, resource, username);
    }
}

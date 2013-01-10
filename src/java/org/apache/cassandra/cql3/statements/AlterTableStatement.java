/*
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
 */
package org.apache.cassandra.cql3.statements;

import java.util.*;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.InvalidRequestException;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class AlterTableStatement extends SchemaAlteringStatement
{
    public static enum Type
    {
        ADD, ALTER, DROP, OPTS
    }

    public final Type oType;
    public final String validator;
    public final ColumnIdentifier columnName;
    private final CFPropDefs cfProps;

    public AlterTableStatement(CFName name, Type type, ColumnIdentifier columnName, String validator, CFPropDefs cfProps)
    {
        super(name);
        this.oType = type;
        this.columnName = columnName;
        this.validator = validator; // used only for ADD/ALTER commands
        this.cfProps = cfProps;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        CFMetaData cfm = meta.clone();

        CFDefinition cfDef = meta.getCfDef();
        CFDefinition.Name name = this.oType == Type.OPTS ? null : cfDef.get(columnName);
        switch (oType)
        {
            case ADD:
                if (cfDef.isCompact)
                    throw new InvalidRequestException("Cannot add new column to a compact CF");
                if (name != null)
                {
                    switch (name.kind)
                    {
                        case KEY_ALIAS:
                        case COLUMN_ALIAS:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                        case COLUMN_METADATA:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
                    }
                }
                cfm.addColumnDefinition(new ColumnDefinition(columnName.key,
                                                             CFPropDefs.parseType(validator),
                                                             null,
                                                             null,
                                                             null,
                                                             cfDef.isComposite ? ((CompositeType)meta.comparator).types.size() - 1 : null));
                break;

            case ALTER:
                if (name == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in CF %s", columnName, columnFamily()));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                        AbstractType<?> newType = CFPropDefs.parseType(validator);
                        if (newType instanceof CounterColumnType)
                            throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", columnName));
                        cfm.keyValidator(newType);
                        break;
                    case COLUMN_ALIAS:
                        assert cfDef.isComposite;

                        List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(((CompositeType) cfm.comparator).types);
                        newTypes.set(name.position, CFPropDefs.parseType(validator));

                        cfm.comparator = CompositeType.getInstance(newTypes);
                        break;
                    case VALUE_ALIAS:
                        cfm.defaultValidator(CFPropDefs.parseType(validator));
                        break;
                    case COLUMN_METADATA:
                        ColumnDefinition column = cfm.getColumnDefinition(columnName.key);
                        column.setValidator(CFPropDefs.parseType(validator));
                        cfm.addColumnDefinition(column);
                        break;
                }
                break;

            case DROP:
                if (cfDef.isCompact)
                    throw new InvalidRequestException("Cannot drop columns from a compact CF");
                if (name == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in CF %s", columnName, columnFamily()));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                    case COLUMN_METADATA:
                        ColumnDefinition toDelete = null;
                        for (ColumnDefinition columnDef : cfm.getColumn_metadata().values())
                        {
                            if (columnDef.name.equals(columnName.key))
                                toDelete = columnDef;
                        }
                        assert toDelete != null;
                        cfm.removeColumnDefinition(toDelete);
                        break;
                }
                break;
            case OPTS:
                if (cfProps == null)
                    throw new InvalidRequestException(String.format("ALTER COLUMNFAMILY WITH invoked, but no parameters found"));

                cfProps.validate();
                cfProps.applyToCFMetadata(cfm);
                break;
        }

        MigrationManager.announceColumnFamilyUpdate(cfm);
    }

    public String toString()
    {
        return String.format("AlterTableStatement(name=%s, type=%s, column=%s, validator=%s)",
                             cfName,
                             oType,
                             columnName,
                             validator);
    }

}

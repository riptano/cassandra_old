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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * A CQL3 term, i.e. a column value with or without bind variables.
 *
 * A Term can be either terminal or non terminal. A term object is one that is typed and is obtained
 * from a raw term (Term.Raw) by poviding the actual receiver to which the term is supposed to be a
 * value of.
 */
public interface Term
{
    /**
     * Collects the column specification for the bind variables in this Term.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param boundNames the list of column specification where to collect the
     * bind variables of this term in.
     */
    public void collectMarkerSpecification(ColumnSpecification[] boundNames);

    /**
     * Bind the values in this term to the values contained in {@code values}.
     * This is obviously a no-op if the term is Terminal.
     *
     * @param values the values to bind markers to.
     * @return the result of binding all the variables of this NonTerminal (or
     * 'this' if the term is terminal).
     */
    public Terminal bind(List<ByteBuffer> values) throws InvalidRequestException;

    /**
     * A shorter for bind(values).get().
     * We expose it mainly because for constants it can avoids allocating a temporary
     * object between the bind and the get (note that we still want to be able
     * to separate bind and get for collections).
     */
    public ByteBuffer bindAndGet(List<ByteBuffer> values) throws InvalidRequestException;

    /**
     * A parsed, non prepared (thus untyped) term.
     *
     * This can be one of:
     *   - a constant
     *   - a collection literal
     *   - a function call
     *   - a marker
     */
    public interface Raw extends AssignementTestable
    {
        /**
         * This method validates this RawTerm is valid for provided column
         * specification and "prepare" this RawTerm, returning the resulting
         * prepared Term.
         *
         * @param receiver the "column" this RawTerm is supposed to be a value of. Note
         * that the ColumnSpecification may not correspond to a real column in the
         * case this RawTerm describe a list index or a map key, etc...
         * @return the prepared term.
         */
        public Term prepare(ColumnSpecification receiver) throws InvalidRequestException;
    }

    /**
     * A terminal term, i.e. one without any bind marker.
     *
     * This can be only one of:
     *   - a constant value
     *   - a collection value
     *
     * Note that a terminal term will always have been type checked, and thus
     * consumer can (and should) assume so.
     */
    public abstract class Terminal implements Term
    {
        public void collectMarkerSpecification(ColumnSpecification[] boundNames) {}
        public Terminal bind(List<ByteBuffer> values) { return this; }

        /**
         * @return the serialized value of this terminal.
         */
        public abstract ByteBuffer get();

        public ByteBuffer bindAndGet(List<ByteBuffer> values) throws InvalidRequestException
        {
            return get();
        }
    }

    /**
     * A non terminal term, i.e. one that contains at least one bind marker.
     *
     * We distinguish between the following type of NonTerminal:
     *   - marker for a constant value
     *   - marker for a collection value (list, set, map)
     *   - a function having bind marker
     */
    public abstract class NonTerminal implements Term
    {
        public ByteBuffer bindAndGet(List<ByteBuffer> values) throws InvalidRequestException
        {
            return bind(values).get();
        }
    }
}

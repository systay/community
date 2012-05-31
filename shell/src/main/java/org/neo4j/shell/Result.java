/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell;

public class Result
{
    private final Continuation continuation;
    private final Object returnValue;

    private Result( Continuation continuation, Object returnValue )
    {
        this.continuation = continuation;
        this.returnValue = returnValue;
    }

    public static Result INPUT_INCOMPLETE = new Result( Continuation.INPUT_INCOMPLETE, null );
    public static Result INPUT_COMPLETE = new Result( Continuation.INPUT_COMPLETE, null );

    public static Result INPUT_COMPLETE( Object returnValue )
    {
        return new Result( Continuation.INPUT_COMPLETE, returnValue );
    }

    public static Result EXIT = new Result( Continuation.EXIT, null );

    public Continuation getContinuation()
    {
        return continuation;
    }

    public Object getReturnValue()
    {
        return returnValue;
    }
}

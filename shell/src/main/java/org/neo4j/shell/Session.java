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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A session (or environment) for a shell client.
 */
public class Session
{
    private final Serializable id;
    private final Map<String, Object> properties = new HashMap<String, Object>();

    public interface SessionInterpreter
    {
        Result execute(Session session, String line, Output out) throws Exception;
    }
    private SessionInterpreter interpreter;

    public void setInterpreter(SessionInterpreter interpreter)
    {
        this.interpreter = interpreter;
    }

    public Result interpret( String line, Output out ) throws Exception
    {
        return interpreter.execute( this, line, out );
    }

    public Session( Serializable id )
    {
        this.id = id;
    }
    
    public Serializable getId()
    {
        return id;
    }
    
	/**
	 * Sets a session value.
	 * @param key the session key.
	 * @param value the value.
	 */
	public void set( String key, Object value )
	{
	    properties.put( key, value );
	}
	
	/**
	 * @param key the key to get the session value for.
	 * @return the value for the {@code key} or {@code null} if not found.
	 */
	public Object get( String key )
	{
	    return properties.get( key );
	}
	
	/**
	 * Removes a value from the session.
	 * @param key the session key to remove.
	 * @return the removed value, or {@code null} if none.
	 */
	public Object remove( String key )
	{
	    return properties.remove( key );
	}
	
	/**
	 * @return all the available session keys.
	 */
	public String[] keys()
	{
	    return properties.keySet().toArray( new String[ properties.size() ] );
	}
	
	/**
	 * Returns the session as a {@link Map} representation. Changes in the
	 * returned instance won't be reflected in the session.
	 * @return the session as a {@link Map}.
	 */
	public Map<String, Object> asMap()
	{
	    return properties;
	}
}

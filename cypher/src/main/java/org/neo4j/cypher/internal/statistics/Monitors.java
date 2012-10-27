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
package org.neo4j.cypher.internal.statistics;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Monitors
{
    public interface Monitor
    {
        void monitorCreated( Class<?> monitorClass, String... tags );

        void monitorListenerException( Throwable throwable );

        public class Adapter implements Monitor
        {
            @Override
            public void monitorCreated( Class<?> monitorClass, String... tags )
            {
            }

            @Override
            public void monitorListenerException( Throwable throwable )
            {
            }
        }
    }

    private Map<Class<?>, List<Object>> monitorListeners = new ConcurrentHashMap<Class<?>, List<Object>>();
    private Map<Class<?>, Map<String, List<Object>>> taggedMonitorListeners = new ConcurrentHashMap<Class<?>, Map<String, List<Object>>>();

    private Monitor monitorsMonitor;

    public Monitors()
    {
        monitorsMonitor = newMonitor( Monitor.class );
    }

    public <T> T newMonitor( Class<T> monitorClass, String... tags )
    {
        List<List<Object>> listenerLists = new ArrayList<List<Object>>();

        // Untagged list first
        listenerLists.add( getMonitorListeners( monitorClass ) );

        // List of listeners for each tag
        for ( String tag : tags )
        {
            listenerLists.add( getTaggedMonitorListeners( monitorClass, tag ) );
        }

        ClassLoader classLoader = monitorClass.getClassLoader();
        MonitorInvocationHandler monitorInvocationHandler = new MonitorInvocationHandler( listenerLists );
        try
        {
            return monitorClass.cast( Proxy.newProxyInstance( classLoader, new Class<?>[]{monitorClass},
                    monitorInvocationHandler ) );
        } finally
        {
            if ( monitorsMonitor != null )
            {
                monitorsMonitor.monitorCreated( monitorClass, tags );
            }
        }
    }

    public void addMonitorListener( Object monitorListener, String... tags )
    {
        for ( Class<?> monitorInterface : getInterfacesOf( monitorListener.getClass() ) )
        {
            if ( tags.length == 0 )
            {
                getMonitorListeners( monitorInterface ).add( monitorListener );
            } else
            {
                for ( String tag : tags )
                {
                    getTaggedMonitorListeners( monitorInterface, tag ).add( monitorListener );
                }
            }
        }
    }

    public void removeMonitorListener( Object monitorListener, String... tags )
    {
        for ( Class<?> monitorInterface : getInterfacesOf( monitorListener.getClass() ) )
        {
            if ( tags.length == 0 )
            {
                getMonitorListeners( monitorInterface ).remove( monitorListener );
            } else
            {
                for ( String tag : tags )
                {
                    getTaggedMonitorListeners( monitorInterface, tag ).remove( monitorListener );
                }
            }
        }
    }

    private <T> List<Object> getMonitorListeners( Class<T> monitorClass )
    {
        List<Object> listeners = monitorListeners.get( monitorClass );
        if ( listeners == null )
        {
            listeners = new ArrayList<Object>();
            monitorListeners.put( monitorClass, listeners );
        }
        return listeners;
    }

    private <T> List<Object> getTaggedMonitorListeners( Class<T> monitorClass, String tag )
    {
        // Get all tagged listener lists first
        Map<String, List<Object>> taggedListeners = taggedMonitorListeners.get( monitorClass );
        if ( taggedListeners == null )
        {
            taggedListeners = new HashMap<String, List<Object>>();
            taggedMonitorListeners.put( monitorClass, taggedListeners );
        }

        // Get listeners for this particular tag
        List<Object> taggedMonitorListeners = taggedListeners.get( tag );
        if ( taggedMonitorListeners == null )
        {
            taggedMonitorListeners = new ArrayList<Object>();
            taggedListeners.put( tag, taggedMonitorListeners );
        }

        return taggedMonitorListeners;
    }

    private Iterable<Class<?>> getInterfacesOf( Class<?> aClass )
    {
        List<Class<?>> interfaces = new ArrayList<Class<?>>();
        while ( aClass != null )
        {
            Collections.addAll( interfaces, aClass.getInterfaces() );
            aClass = aClass.getSuperclass();
        }
        return interfaces;
    }


    private class MonitorInvocationHandler implements InvocationHandler
    {
        private List<List<Object>> listenerLists;

        public MonitorInvocationHandler( List<List<Object>> listenerLists )
        {
            this.listenerLists = listenerLists;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            invokeMonitorListeners( method, args );
            return null;
        }

        private void invokeMonitorListeners( Method method, Object[] args )
        {
            for ( List<Object> listenerList : listenerLists )
            {
                for ( Object monitorListener : listenerList )
                {
                    try
                    {
                        method.invoke( monitorListener, args );
                    } catch ( Throwable e )
                    {
                        if ( !method.getDeclaringClass().equals( Monitor.class ) )
                        {
                            monitorsMonitor.monitorListenerException( e );
                        }
                    }
                }
            }
        }
    }
}

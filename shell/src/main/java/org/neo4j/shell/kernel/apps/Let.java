package org.neo4j.shell.kernel.apps;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Result;
import org.neo4j.shell.Session;

public class Let extends ReadOnlyGraphDatabaseApp
{
    Pattern p = Pattern.compile( "^([a-zA-Z0-9_])*[ \\\\t]*=[ \\\\t]*(.*)" );

    @Override
    protected Result exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        String lineWithoutApp = parser.getLineWithoutApp();
        Matcher m = p.matcher( lineWithoutApp );
        if ( !m.matches() )
        {
            out.println( "LET xyz = START...RETURN x;" );
            return Result.INPUT_COMPLETE;
        }

        String variable = m.group( 1 ).trim();
        String cmd = m.group( 2 ).trim();

        Result result = session.interpret( cmd, out );

        if ( result.getContinuation() == Continuation.INPUT_COMPLETE )
        {
            Map<String, Object> params = getOrCreateCypherParams( session );
            Object returnValue = result.getReturnValue();
            if( returnValue == null)
            {
                params.remove( variable );
            }
            else if ( returnValue instanceof ExecutionResult )
            {
                ExecutionResult executionResult = (ExecutionResult) returnValue;
                if(executionResult.columns().size()>2)
                {
                    out.println( "Can only assign a single column Cypher result to a variable" );
                    return Result.INPUT_COMPLETE;
                }
            } else {
                params.put( variable, returnValue );
            }
        }

        return result;
    }

    private Map<String,Object> getOrCreateCypherParams( Session session )
    {
        final String CYPHER_PARAMS_KEY = "CYPHER_PARAMS";
        if(session.get( CYPHER_PARAMS_KEY ) == null) {
            Map<String,Object> params = new HashMap<String, Object>(  );
            session.set( CYPHER_PARAMS_KEY, params );
            return params;
        }
        else
        {
            return (Map<String, Object>) session.get( CYPHER_PARAMS_KEY );
        }
    }
}

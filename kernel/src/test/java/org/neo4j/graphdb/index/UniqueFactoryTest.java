package org.neo4j.graphdb.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class UniqueFactoryTest
{
    @Test
    public void shouldUseConcurrentlyCreatedNode()
    {
        // given
        GraphDatabaseService graphdb = mock( GraphDatabaseService.class );
        @SuppressWarnings( "unchecked" )
        Index<Node> index = mock( Index.class );
        Transaction tx = mock( Transaction.class );
        when( graphdb.beginTx() ).thenReturn( tx );
        when( index.getGraphDatabase() ).thenReturn( graphdb );
        @SuppressWarnings( "unchecked" )
        IndexHits<Node> getHits = mock( IndexHits.class );
        when( index.get( "key1", "value1" ) ).thenReturn( getHits );
        Node createdNode = mock( Node.class );
        when( graphdb.createNode() ).thenReturn( createdNode );
        Node concurrentNode = mock( Node.class );
        when( index.putIfAbsent( createdNode, "key1", "value1" ) ).thenReturn( concurrentNode );
        UniqueFactory.UniqueNodeFactory unique = new UniqueFactory.UniqueNodeFactory( index )
        {
            @Override
            protected void initialize( Node created, Map<String, Object> properties )
            {
                fail( "we did not create the node, so it should not be initialized" );
            }
        };

        // when
        Node node = unique.getOrCreate( "key1", "value1" );

        // then
        assertSame(node, concurrentNode);
        verify( index ).get( "key1", "value1" );
        verify( index ).putIfAbsent( createdNode, "key1", "value1" );
        verify( graphdb, times( 1 ) ).createNode();
        verify( tx ).success();
    }

    @Test
    public void shouldCreateNodeAndIndexItIfMissing()
    {
        // given
        GraphDatabaseService graphdb = mock( GraphDatabaseService.class );
        @SuppressWarnings( "unchecked" )
        Index<Node> index = mock( Index.class );
        Transaction tx = mock( Transaction.class );
        when( graphdb.beginTx() ).thenReturn( tx );
        when( index.getGraphDatabase() ).thenReturn( graphdb );
        @SuppressWarnings( "unchecked" )
        IndexHits<Node> getHits = mock( IndexHits.class );

        when( index.get( "key1", "value1" ) ).thenReturn( getHits );
        Node indexedNode = mock( Node.class );
        when( graphdb.createNode() ).thenReturn( indexedNode );
        final AtomicBoolean initializeCalled = new AtomicBoolean( false );
        UniqueFactory.UniqueNodeFactory unique = new UniqueFactory.UniqueNodeFactory( index )
        {
            @Override
            protected void initialize( Node created, Map<String, Object> properties )
            {
                initializeCalled.set( true );
                assertEquals( Collections.singletonMap( "key1", "value1" ), properties );
            }
        };

        // when
        Node node = unique.getOrCreate( "key1", "value1" );

        // then
        assertSame(node, indexedNode);
        verify( index ).get( "key1", "value1" );
        verify( index ).putIfAbsent( indexedNode, "key1", "value1" );
        verify( graphdb, times( 1 ) ).createNode();
        verify( tx ).success();
        assertTrue( "Node not initialized", initializeCalled.get() );
    }

    @Test
    public void shouldNotTouchTransactionsIfAlreadyInIndex()
    {
        GraphDatabaseService graphdb = mock( GraphDatabaseService.class );
        @SuppressWarnings( "unchecked" )
        Index<Node> index = mock( Index.class );
        when( index.getGraphDatabase() ).thenReturn( graphdb );
        @SuppressWarnings( "unchecked" )
        IndexHits<Node> getHits = mock( IndexHits.class );
        when( index.get( "key1", "value1" ) ).thenReturn( getHits );
        Node indexedNode = mock( Node.class );
        when( getHits.getSingle() ).thenReturn( indexedNode );

        UniqueFactory.UniqueNodeFactory unique = new UniqueFactory.UniqueNodeFactory( index )
        {
            @Override
            protected void initialize( Node created, Map<String, Object> properties )
            {
                fail( "we did not create the node, so it should not be initialized" );
            }
        };

        // when
        Node node = unique.getOrCreate( "key1", "value1" );

        // then
        assertSame( node, indexedNode );
        verify( index ).get( "key1", "value1" );
    }
}

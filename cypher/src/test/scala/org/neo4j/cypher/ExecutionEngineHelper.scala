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
package org.neo4j.cypher

import internal.commands.Query
import internal.pipes.QueryState
import org.junit.Before
import org.scalatest.Assertions


trait ExecutionEngineHelper extends GraphDatabaseTestBase {

  var engine: ExecutionEngine = null

  @Before
  def executionEngineHelperInit() {
    engine = new ExecutionEngine(graph)
  }

  def execute(query: Query, params:(String,Any)*) = {
    val result = engine.execute(query, params.toMap)
    result
  }


  def parseAndExecute(q: String, params: (String, Any)*): ExecutionResult = {
    val plan = engine.prepare(q)
    
    plan.execute(params.toMap)
  }

  def executeScalar[T](q: String, params: (String, Any)*):T = engine.execute(q, params.toMap).toList match {
    case List(m) => if (m.size!=1)
      fail("expected scalar value: " + m)
      else m.head._2.asInstanceOf[T]
    case x => fail(x.toString())
  }



}

trait StatsAssertions extends Assertions {
  protected def assertStats(state:QueryState,
                            createdNode: Int = 0,
                            deletedNode: Int = 0,
                            createdRelationship: Int = 0,
                            deletedRelationship: Int = 0,
                            setProperty: Int = 0) {
    val stats = state.updateCounter.toStats
    assert(stats.nodesCreated === createdNode, "Nodes created didn't match")
    assert(stats.deletedNodes === deletedNode, "Nodes deleted didn't match")
    assert(stats.relationshipsCreated === createdRelationship, "Relationships created didn't match")
    assert(stats.deletedRelationships === deletedRelationship, "Relationship deleted didn't match")
    assert(stats.propertiesSet === setProperty, "Properties set didn't match")
  }
}
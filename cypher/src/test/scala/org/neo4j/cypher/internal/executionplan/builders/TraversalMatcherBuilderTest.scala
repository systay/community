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
package org.neo4j.cypher.internal.executionplan.builders

import org.junit.{Ignore, Before, Test}
import org.neo4j.cypher.internal.commands._
import org.neo4j.graphdb.Direction
import org.scalatest.Assertions
import org.neo4j.graphdb.Direction._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.expressions.Literal
import org.neo4j.cypher.internal.executionplan.builders.TraversalMatcherBuilder.LongestPathResult
import org.neo4j.cypher.internal.pipes.matching.ExpanderStep
import org.neo4j.cypher.internal.executionplan.builders.TraversalMatcherBuilder.BoundPoint
import org.neo4j.cypher.internal.executionplan.builders.TraversalMatcherBuilder.WrappingTrail
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.pipes.NullPipe
import org.neo4j.cypher.internal.parser.v1_9.CypherParserImpl

class TraversalMatcherBuilderTest extends GraphDatabaseTestBase with Assertions with BuilderTest {
  var builder:TraversalMatcherBuilder = null
  val A = withName("A")
  val B = withName("B")
  val C = withName("C")
  val D = withName("D")

  /*
          (b2)
            ^
            |
          [:D]
            |
 (a)-[:A]->(b)-[:B]->(c)-[:C]->(d)
  */
  val AtoB = RelatedTo("a", "b", "pr1", Seq("A"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoC = RelatedTo("b", "c", "pr2", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())
  val CtoD = RelatedTo("c", "d", "pr3", Seq("C"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoB2 = RelatedTo("b", "b2", "pr4", Seq("D"), Direction.OUTGOING, optional = false, predicate = True())

  @Before def init() {
    builder = new TraversalMatcherBuilder(graph)
  }

  @Test def find_longest_path_for_single_pattern() {
    val expected = ExpanderStep(0, Seq(A), Direction.INCOMING, None)


    TraversalMatcherBuilder.findLongestPath(Seq(AtoB), Seq("a", "b")) match {
      case Some(lpr@LongestPathResult("a", Some("b"), remains, lp)) => assert(lpr.step === expected.reverse())
      case Some(lpr@LongestPathResult("b", Some("a"), remains, lp)) => assert(lpr.step === expected)
      case _                                                      => fail("Didn't find any paths")
    }
  }

  @Test def find_longest_path_between_two_points() {
    val forward2 = ExpanderStep(0, Seq(A), Direction.INCOMING, None)
    val forward1 = ExpanderStep(1, Seq(B), Direction.INCOMING, Some(forward2))

    val backward2 = ExpanderStep(0, Seq(B), Direction.OUTGOING, None)
    val backward1 = ExpanderStep(1, Seq(A), Direction.OUTGOING, Some(backward2))

    TraversalMatcherBuilder.findLongestPath(Seq(AtoB, BtoC, BtoB2), Seq("a", "c")) match {
      case Some(lpr@LongestPathResult("a", Some("c"), remains, lp)) => assert(lpr.step === backward1)
      case Some(lpr@LongestPathResult("c", Some("a"), remains, lp)) => assert(lpr.step === forward1)
      case _                                                        => fail("Didn't find any paths")
    }
  }

  @Ignore @Test def find_longest_path_with_single_start() {
    val pr3 = ExpanderStep(0, Seq(C), OUTGOING, None)
    val pr2 = ExpanderStep(1, Seq(B), OUTGOING, Some(pr3))
    val pr1 = ExpanderStep(2, Seq(A), OUTGOING, Some(pr2))

    TraversalMatcherBuilder.findLongestPath(Seq(AtoB, BtoC, BtoB2, CtoD), Seq("a")) match {
      case Some(lpr@LongestPathResult("a", None, Seq(BtoB2), lp)) => assert(lpr.step === pr1)
      case _                                                      => fail("Didn't find any paths")
    }
  }

  @Test def decompose_simple_path() {
    val nodeA    = createNode("A")
    val nodeB    = createNode("B")
    val rel      = relate(nodeA, nodeB, "LINK_T")

    val kernPath = Seq(nodeA, rel, nodeB).reverse
    val path     = WrappingTrail(BoundPoint("a"), Direction.OUTGOING, "link", Seq("LINK_T"), "b")

    val resultMap = path.decompose(kernPath)
    assert(resultMap === Map("a" -> nodeA, "b" -> nodeB, "link" -> rel))
  }

  @Test def decompose_little_longer_path() {
    val nodeA    = createNode("A")
    val nodeB    = createNode("B")
    val nodeC    = createNode("C")
    val rel1      = relate(nodeA, nodeB, "LINK_T")
    val rel2      = relate(nodeB, nodeC, "LINK_T")

    val kernPath = Seq(nodeA, rel1, nodeB, rel2, nodeC).reverse
    val path     =
      WrappingTrail(
        WrappingTrail(BoundPoint("a"), Direction.OUTGOING, "link1", Seq("LINK_T"), "b"),
        Direction.OUTGOING, "link2", Seq("LINK_T"), "c")

    val resultMap = path.decompose(kernPath)
    assert(resultMap === Map("a" -> nodeA, "b" -> nodeB, "c" -> nodeC,
                             "link1" -> rel1, "link2" -> rel2))
  }

  @Test def should_not_accept_queries_without_patterns() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndex("n", "index", Literal("key"), Literal("expression"))))
    )

    assertFalse("This query should not be accepted", builder.canWorkWith(plan(new NullPipe, q)))
  }

  @Test def should_not_accept_queries_with_single_start_point() {
    val q = query("START me=node:node_auto_index(name = 'Jane') " +
      "MATCH me-[:jane_knows]->friend-[:has]->status " +
      "RETURN me")

    assertFalse("This query should not be accepted", builder.canWorkWith(plan(new NullPipe, q)))
  }

  @Test def should_not_crash() {
    val q = query("START me=node:node_auto_index(name = 'Jane') " +
      "MATCH me-[:jane_knows*]->friend-[:has]->status " +
      "RETURN me")

    assertFalse("This query should not be accepted", builder.canWorkWith(plan(new NullPipe, q)))
  }

  @Test def should_not_accept_queries_with_varlength_paths() {
    val q = query("START me=node:node_auto_index(name = 'Tarzan'), you=node:node_auto_index(name = 'Jane') " +
      "MATCH me-[:LOVES*]->banana-[:LIKES]->you " +
      "RETURN me")

    assertFalse("This query should not be accepted", builder.canWorkWith(plan(new NullPipe, q)))
  }

  @Test def should_handle_loops() {
    val q = query("START me=node:node_auto_index(name = 'Tarzan'), you=node:node_auto_index(name = 'Jane') " +
      "MATCH me-[:LIKES]->(u1)<-[:LIKES]->you, me-[:HATES]->(u2)<-[:HATES]->you " +
      "RETURN me")

    assertTrue("This query should be accepted", builder.canWorkWith(plan(new NullPipe, q)))
  }

  val parser = new CypherParserImpl
  private def query(text:String):PartiallySolvedQuery=PartiallySolvedQuery(parser.parse(text))
}
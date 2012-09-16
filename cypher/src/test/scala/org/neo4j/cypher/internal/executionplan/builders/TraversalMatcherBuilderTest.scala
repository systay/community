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

import org.junit.Test
import org.neo4j.cypher.internal.commands.RelatedTo
import org.neo4j.graphdb.Direction
import org.scalatest.Assertions
import org.neo4j.graphdb.Direction._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.internal.pipes.matching.ExpanderStep
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.executionplan.builders.TraversalMatcherBuilder.LongestPathResult

class TraversalMatcherBuilderTest extends Assertions {
  val builder = new TraversalMatcherBuilder
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


  @Test def find_longest_path_for_single_pattern() {
    val expected = ExpanderStep(0, Seq(A), Direction.INCOMING, None)


    TraversalMatcherBuilder.findLongestPath(Seq(AtoB), Seq("a", "b")) match {
      case Some(LongestPathResult(step, "a", Some("b"), remains)) => assert(step === expected.reverse())
      case Some(LongestPathResult(step, "b", Some("a"), remains)) => assert(step === expected)
      case _                                                      => fail("Didn't find any paths")
    }
  }

  @Test def find_longest_path_between_two_points() {
    val forward2 = ExpanderStep(0, Seq(A), Direction.INCOMING, None)
    val forward1 = ExpanderStep(1, Seq(B), Direction.INCOMING, Some(forward2))

    val backward2 = ExpanderStep(0, Seq(B), Direction.OUTGOING, None)
    val backward1 = ExpanderStep(1, Seq(A), Direction.OUTGOING, Some(backward2))

    TraversalMatcherBuilder.findLongestPath(Seq(AtoB, BtoC, BtoB2), Seq("a", "c")) match {
      case Some(LongestPathResult(step, "a", Some("c"), remains)) => assert(step === backward1)
      case Some(LongestPathResult(step, "c", Some("a"), remains)) => assert(step === forward1)
      case _                                                      => fail("Didn't find any paths")
    }
  }

  @Test def find_longest_path_with_single_start() {
    val pr3 = ExpanderStep(0, Seq(C), OUTGOING, None)
    val pr2 = ExpanderStep(1, Seq(B), OUTGOING, Some(pr3))
    val pr1 = ExpanderStep(2, Seq(A), OUTGOING, Some(pr2))

    TraversalMatcherBuilder.findLongestPath(Seq(AtoB, BtoC, BtoB2, CtoD), Seq("a")) match {
      case Some(LongestPathResult(step, "a", None, Seq(BtoB2))) => assert(step === pr1)
      case _                                                    => fail("Didn't find any paths")
    }
  }
}
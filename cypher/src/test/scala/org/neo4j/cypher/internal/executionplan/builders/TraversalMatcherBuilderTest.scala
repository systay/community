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

class TraversalMatcherBuilderTest extends Assertions {
  val builder = new TraversalMatcherBuilder
  val A = withName("A")
  val B = withName("B")

  /*
      (b2)
        ^
        |
 (a)-->(b)-->(c)-->(d)
  */
  val AtoB = RelatedTo("a", "b", "pr1", Seq("A"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoC = RelatedTo("b", "c", "pr2", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())
  val CtoD = RelatedTo("c", "d", "pr3", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoB2 = RelatedTo("b", "b2", "pr4", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())


  @Test def find_longest_path_for_single_pattern() {
    val expected = ExpanderStep(0, Seq(A), Direction.INCOMING, None)

    val step = TraversalMatcherBuilder.findLongestPath(Seq(AtoB), Seq("a", "b"))

    assert(step === Some(expected))
  }

  @Test def find_longest_path_between_two_points() {
    val step2 = ExpanderStep(0, Seq(B), Direction.OUTGOING, None)
    val step1 = ExpanderStep(1, Seq(A), Direction.OUTGOING, Some(step2))

    assert(TraversalMatcherBuilder.findLongestPath(Seq(AtoB, BtoC, BtoB2), Seq("a", "c")) === Some(step1))
  }

  @Test def find_longest_path_with_single_start() {
    val pr2 = ExpanderStep(0, Seq(B), OUTGOING, None)
    val pr1 = ExpanderStep(1, Seq(A), OUTGOING, Some(pr2))

    val path = TraversalMatcherBuilder.findLongestPath(Seq(AtoB, BtoC, BtoB2), Seq("a"))

    assert(path  === Some(pr1))
  }
}
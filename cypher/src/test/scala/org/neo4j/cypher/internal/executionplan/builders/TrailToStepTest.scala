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
import org.neo4j.cypher.internal.commands._
import expressions.{Literal, Property}
import org.neo4j.graphdb.{RelationshipType, Direction}
import org.scalatest.Assertions
import org.neo4j.graphdb.Direction._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.pipes.matching.{SingleStep, ExpanderStep}
import org.neo4j.cypher.internal.commands.True

class TrailToStepTest extends GraphDatabaseTestBase with Assertions with BuilderTest {
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
            |
          [:A*]
            |
            v
           (e)

 case class VarLengthRelatedTo(pathName: String,
                              start: String,
                              end: String,
                              minHops: Option[Int],
                              maxHops: Option[Int],
                              relTypes: Seq[String],
                              direction: Direction,
                              relIterator: Option[String],
                              optional: Boolean,
                              predicate: Predicate) extends PathPattern {


  */
  val AtoB = RelatedTo("a", "b", "pr1", Seq("A"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoC = RelatedTo("b", "c", "pr2", Seq("B"), Direction.OUTGOING, optional = false, predicate = True())
  val CtoD = RelatedTo("c", "d", "pr3", Seq("C"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoB2 = RelatedTo("b", "b2", "pr4", Seq("D"), Direction.OUTGOING, optional = false, predicate = True())
  val BtoE = VarLengthRelatedTo("p", "b", "e", None, None, Seq("A"), Direction.OUTGOING, None, optional = false, predicate = True())

  @Test def find_longest_path_for_single_pattern() {
    val expected = step(0, Seq(A), Direction.INCOMING, None)

    val steps = SingleStepTrail(BoundPoint("b"), Direction.INCOMING, "pr1", Seq("A"), "a", Seq.empty, AtoB).toSteps(0).get

    assert(steps === expected)
  }

  @Test def find_longest_path_between_two_points() {
    val boundPoint = BoundPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.INCOMING, "pr2", Seq("B"), "b", Seq.empty, BtoC)
    val first = SingleStepTrail(second, Direction.INCOMING, "pr1", Seq("A"), "a", Seq.empty, AtoB)

    val backward2 = step(1, Seq(B), Direction.INCOMING, None)
    val backward1 = step(0, Seq(A), Direction.INCOMING, Some(backward2))

    assert(first.toSteps(0).get === backward1)
  }

  @Test def find_longest_path_between_two_points_with_a_predicate() {

    //()<-[r1:A]-(a)<-[r2:B]-()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val r1Pred = Equals(Property("pr1", "prop"), Literal(42))
    val r2Pred = Equals(Property("pr2", "prop"), Literal("FOO"))
    val predicates = Seq(r1Pred, r2Pred)

    val boundPoint = BoundPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.INCOMING, "pr2", Seq("B"), "b", predicates, BtoC)
    val first = SingleStepTrail(second, Direction.INCOMING, "pr1", Seq("A"), "a", predicates, AtoB)

    val backward2 = step(1, Seq(B), Direction.INCOMING, None, relPredicate = r2Pred)
    val backward1 = step(0, Seq(A), Direction.INCOMING, Some(backward2), relPredicate = r1Pred)

    assert(first.toSteps(0).get === backward1)
  }

  @Test def find_longest_path_between_two_points_with_a_node_predicate() {
    //()-[pr1:A]->(a)-[pr2:B]->()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val nodePred = Equals(Property("b", "prop"), Literal(42))

    val forward2 = step(1, Seq(B), Direction.INCOMING, None, nodePredicate = nodePred)
    val forward1 = step(0, Seq(A), Direction.INCOMING, Some(forward2))

    val predicates = Seq(nodePred)

    val boundPoint = BoundPoint("c")
    val second = SingleStepTrail(boundPoint, Direction.INCOMING, "pr2", Seq("B"), "b", predicates, BtoC)
    val first = SingleStepTrail(second, Direction.INCOMING, "pr1", Seq("A"), "a", predicates, AtoB)


    assert(first.toSteps(0).get === forward1)
  }

  @Test def should_not_accept_trails_with_bound_points_in_the_middle() {
    //()-[pr1:A]->(a)-[pr2:B]->()
    //WHERE r1.prop = 42 AND r2.prop = "FOO"

    val LongestTrail(_,_,trail) = TrailBuilder.findLongestTrail(Seq(AtoB, BtoC), Seq("a", "b", "c"), Seq()).get

    assert(trail.size === 1)
  }

  @Test def find_longest_path_with_single_start() {
    val pr3 = step(2, Seq(A), OUTGOING, None)
    val pr2 = step(1, Seq(B), OUTGOING, Some(pr3))
    val pr1 = step(0, Seq(C), OUTGOING, Some(pr2))

    val boundPoint = BoundPoint("a")
    val third = SingleStepTrail(boundPoint, Direction.OUTGOING, "pr1", Seq("A"), "b", Seq.empty, AtoB)
    val second = SingleStepTrail(third, Direction.OUTGOING, "pr2", Seq("B"), "c", Seq.empty, BtoC)
    val first = SingleStepTrail(second, Direction.OUTGOING, "pr3", Seq("C"), "d", Seq.empty, CtoD)

    assert(first.toSteps(0).get === pr1)
  }

  private def step(id: Int,
                   typ: Seq[RelationshipType],
                   direction: Direction,
                   next: Option[ExpanderStep],
                   nodePredicate: Predicate = True(),
                   relPredicate: Predicate = True()) =
    SingleStep(id, typ, direction, next, relPredicate = relPredicate, nodePredicate = nodePredicate)
}
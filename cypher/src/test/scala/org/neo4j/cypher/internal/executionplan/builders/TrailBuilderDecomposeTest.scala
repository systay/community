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
import org.neo4j.graphdb.Direction
import org.scalatest.Assertions
import org.neo4j.cypher.GraphDatabaseTestBase

class TrailBuilderDecomposeTest extends GraphDatabaseTestBase with Assertions with BuilderTest {
  @Test def decompose_simple_path() {
    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val rel = relate(nodeA, nodeB, "LINK_T")

    val kernPath = Seq(nodeA, rel, nodeB).reverse
    val path = SingleStepTrail(BoundPoint("a"), Direction.OUTGOING, "link", Seq("LINK_T"), "b", None, None, null)

    val resultMap = path.decompose(kernPath)
    assert(resultMap === List(Map("a" -> nodeA, "b" -> nodeB, "link" -> rel)))
  }

  @Test def decompose_little_longer_path() {
    //c-[link2]->b-[link1]->a

    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val nodeC = createNode("C")
    val rel1 = relate(nodeA, nodeB, "LINK_T")
    val rel2 = relate(nodeB, nodeC, "LINK_T")

    val kernPath = Seq(nodeA, rel1, nodeB, rel2, nodeC).reverse
    val path =
      SingleStepTrail(
        SingleStepTrail(BoundPoint("a"), Direction.OUTGOING, "link1", Seq("LINK_T"), "b", None, None, null),
        Direction.OUTGOING, "link2", Seq("LINK_T"), "c", None, None, null)

    val resultMap = path.decompose(kernPath)
    assert(resultMap === List(Map("a" -> nodeA, "b" -> nodeB, "c" -> nodeC,
      "link1" -> rel1, "link2" -> rel2)))
  }

  @Test def decompose_single_varlength_step() {
    //Given:
    //Pattern: a-[:A*1..2]->b
    //   Path: 0-[:A]->1

    val nodeA = createNode()
    val nodeB = createNode()
    val rel1 = relate(nodeA, nodeB, "A")

    val kernPath = Seq(nodeA, rel1, nodeB)
    val path =
      VariableLengthStepTrail(BoundPoint("b"), Direction.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //When
    val resultMap = path.decompose(kernPath)

    //Then
    assert(resultMap === List(Map("a" -> nodeA, "b" -> nodeB, "p" -> kernPath)))
  }

  @Test def decompose_single_step_follow_with_varlength() {
    //Given:
    //Pattern: x-[r1:B]->a-[:A*1..2]->b
    //   Path: 0-[:B]->1-[:A]->2

    val node0 = createNode()
    val node1 = createNode()
    val node2 = createNode()
    val rel1 = relate(node0, node1, "B")
    val rel2 = relate(node1, node2, "A")

    val kernPath = Seq(node0, rel1, node1, rel2, node2)
    val expectedPath = Seq(node1, rel2, node2)
    val path =
      SingleStepTrail(
        VariableLengthStepTrail(BoundPoint("b"), Direction.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null),
        dir = Direction.OUTGOING, rel = "r1", typ = Seq("B"), end = "x", relPred = None, nodePred = None, pattern = null)

    //When
    val resultMap = path.decompose(kernPath)

    //Then
    assert(resultMap === List(Map("x" -> node0, "a" -> node1, "b" -> node2, "r1" -> rel1, "p" -> expectedPath)))
  }

  @Test def decompose_varlength_followed_by_single_step() {
    //Given:
    //Pattern: a-[:A*1..2]->b<-[r1:B]-x
    //   Path: 0-[:A     ]->1<-[  :B]-2

    val node0 = createNode()
    val node1 = createNode()
    val node2 = createNode()
    val rel0 = relate(node0, node1, "A")
    val rel1 = relate(node2, node1, "B")

    val kernPath = Seq(node0, rel0, node1, rel1, node2)
    val expectedPath = Seq(node0, rel0, node1)
    val bound = BoundPoint("x")
    val single = SingleStepTrail(bound, Direction.INCOMING, "r1", Seq("B"), "b", None, None, null)
    val path = VariableLengthStepTrail(single, Direction.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //When
    val resultMap = path.decompose(kernPath)

    //Then
    assert(resultMap === List(Map("x" -> node2, "a" -> node0, "b" -> node1, "r1" -> rel1, "p" -> expectedPath)))
  }

  @Test def multi_step_variable_length_decompose() {
    //Given:
    //Pattern: a-[:A*1..2]->b<-[:*1..2]-x
    //   Path: 0-[:A]->1-[:A]->2

    val node0 = createNode()
    val node1 = createNode()
    val node2 = createNode()
    val rel0 = relate(node0, node1, "A")
    val rel1 = relate(node1, node2, "A")

    val input = Seq(node0, rel0, node1, rel1, node2)
    val expectedPath = input

    val bound = BoundPoint("b")
    val path = VariableLengthStepTrail(bound, Direction.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //When
    val resultMap = path.decompose(input)

    //Then
    assert(resultMap === List(Map("a" -> node0, "b" -> node2, "p" -> expectedPath)))
  }
}
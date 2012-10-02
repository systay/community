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

class TrailBuilderDecomposeTest extends GraphDatabaseTestBase with Assertions with BuilderTest {
  @Test def decompose_simple_path() {
    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val rel = relate(nodeA, nodeB, "LINK_T")

    val kernPath = Seq(nodeA, rel, nodeB).reverse
    val path = SingleStepTrail(BoundPoint("a"), Direction.OUTGOING, "link", Seq("LINK_T"), "b", None, None, null)

    val resultMap = path.decompose(kernPath)
    assert(resultMap === Map("a" -> nodeA, "b" -> nodeB, "link" -> rel))
  }

  @Test def decompose_little_longer_path() {
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
    assert(resultMap === Map("a" -> nodeA, "b" -> nodeB, "c" -> nodeC,
      "link1" -> rel1, "link2" -> rel2))
  }
}
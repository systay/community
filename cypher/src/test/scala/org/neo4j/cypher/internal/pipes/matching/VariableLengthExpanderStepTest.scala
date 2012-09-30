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
package org.neo4j.cypher.internal.pipes.matching

import org.junit.Test
import org.neo4j.graphdb.{RelationshipType, DynamicRelationshipType, Direction}
import org.scalatest.Assertions
import org.neo4j.cypher.internal.commands.True

class VariableLengthExpanderStepTest extends Assertions {

  private def step(id: Int,
                   typ: Seq[RelationshipType],
                   direction: Direction,
                   next: Option[ExpanderStep]) = SingleStep(id, typ, direction, next, True(), True())

  private def varStep(id: Int,
                      typ: Seq[RelationshipType],
                      direction: Direction,
                      min: Option[Int],
                      max: Option[Int],
                      next: Option[ExpanderStep]) = VarLengthStep(id, typ, direction, next, True(), True())

  val A = DynamicRelationshipType.withName("A")
  val B = DynamicRelationshipType.withName("B")
  val C = DynamicRelationshipType.withName("C")

  @Test def reverse_single_step() {
    // ()-[:A*]->()
    val step = varStep(0, Seq(A), Direction.OUTGOING, None, None, None)

    // ()<-[:A*]-()
    val reversed = varStep(0, Seq(A), Direction.INCOMING, None, None, None)

    assert(step.reverse() === reversed)
    assert(reversed.reverse() === step)
  }

  @Test def reverse_two_steps() {
    // ()-[:A*]->()<-[:B*]-()
    val step2 = varStep(1, Seq(B), Direction.INCOMING, None, None, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, None, None, Some(step2))

    // ()-[:B*]->()<-[:A*]-()
    val reversed2 = varStep(0, Seq(A), Direction.INCOMING, None, None, None)
    val reversed1 = varStep(1, Seq(B), Direction.OUTGOING, None, None, Some(reversed2))

    assert(step1.reverse() === reversed1)
    assert(reversed1.reverse() === step1)
  }

  @Test def reverse_mixed_steps() {
    // ()-[:A*]->()-[:B]-()
    val step2 = step(1, Seq(B), Direction.INCOMING, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, None, None, Some(step2))

    // ()-[:B]-()<-[:A*]-()
    val reversed2 = varStep(0, Seq(A), Direction.INCOMING, None, None, None)
    val reversed1 = step(1, Seq(B), Direction.OUTGOING, Some(reversed2))

    assert(step1.reverse() === reversed1)
    assert(reversed1.reverse() === step1)
  }
}
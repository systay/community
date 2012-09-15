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
import org.neo4j.graphdb.{DynamicRelationshipType, Direction}
import org.scalatest.Assertions

class ExpanderStepTest extends Assertions {

  val A = DynamicRelationshipType.withName("A")
  val B = DynamicRelationshipType.withName("B")
  val C = DynamicRelationshipType.withName("C")

  val c = ExpanderStep(2, Seq(C), Direction.INCOMING, None)
  val b = ExpanderStep(1, Seq(B), Direction.BOTH, Some(c))
  val a = ExpanderStep(0, Seq(A), Direction.OUTGOING, Some(b))

  val aR = ExpanderStep(0, Seq(A), Direction.INCOMING, None)
  val bR = ExpanderStep(1, Seq(B), Direction.BOTH, Some(aR))
  val cR = ExpanderStep(2, Seq(C), Direction.OUTGOING, Some(bR))


  @Test def reverse() {
    assert(a.reverse() === cR)
    assert(cR.reverse() === a)
  }
}
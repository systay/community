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
package org.neo4j.cypher.internal.mutation

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.{Entity, CreateRelationshipStartItem}
import org.hamcrest.CoreMatchers._
import org.neo4j.graphdb.{PropertyContainer, Direction}

class RelateResultTest {
  val A = RelateLink("a", "b", "r", "X", Direction.OUTGOING)
  val B = RelateLink("a", "c", "r", "X", Direction.OUTGOING)
  val C = RelateLink("a", "d", "r", "X", Direction.OUTGOING)
  val cmd1 = UpdateWrapper(Seq("a", "b"), CreateRelationshipStartItem("r", (Entity("a"), Map()), (Entity("b"), Map()), "x", Map()))
  val cmd2 = UpdateWrapper(Seq("a", "c"), CreateRelationshipStartItem("r", (Entity("a"), Map()), (Entity("c"), Map()), "x", Map()))

  val done1 = Done()
  val done2 = Done()
  val cantAdvance1 = CanNotAdvance(leftToDo = Seq(A))
  val cantAdvance2 = CanNotAdvance(leftToDo = Seq(B))
  val traverse1 = Traverse(leftToDo = Seq(A), result = Seq("a" -> null))
  val traverse2 = Traverse(leftToDo = Seq(C), result = Seq("b" -> null))
  val update1 = Update(Seq(cmd1), () => Seq(), Seq(C))
  val update2 = Update(Seq(cmd2), () => Seq(), Seq(B))


  @Test def testDone() {
    assertThat(done1 reduceWith done2, instanceOf(classOf[Done]))
    assertThat(done1 reduceWith cantAdvance1, equalTo[RelateResult](cantAdvance1))
    assertThat(done1 reduceWith traverse1, equalTo[RelateResult](traverse1))
    assertThat(done1 reduceWith update1, equalTo[RelateResult](update1))
  }

  @Test def cant_advanced_with_cant_advance() {
    val reduced = cantAdvance1 reduceWith cantAdvance2
    assertThat(reduced.leftToDo, equalTo(Seq(A, B)))
    assertThat(reduced, instanceOf(classOf[CanNotAdvance]))
  }

  @Test def cant_advanced_with_traverse() {
    val reduced = cantAdvance1 reduceWith traverse2
    assertThat(reduced.leftToDo.toSet, equalTo(Set(A, C)))
    assertThat(reduced, instanceOf(classOf[Traverse]))
  }

  @Test def cant_advanced_with_update() {
    val reduced = cantAdvance1 reduceWith update1
    assertThat(reduced.leftToDo.toSet, equalTo(Set(A, C)))
    assertThat(reduced, instanceOf(classOf[Update]))
  }

  @Test def traverse_w_traverse() {
    val reduced = traverse1 reduceWith traverse2
    assertThat(reduced.leftToDo.toSet, equalTo(Set(A, C)))
    assertThat(reduced, instanceOf(classOf[Traverse]))
    assertThat(reduced.asInstanceOf[Traverse].result, equalTo(Seq[(String, PropertyContainer)]("a" -> null, "b" -> null)))
  }

  @Test def traverse_w_update() {
    val reduced = traverse1 reduceWith update2
    assertThat(reduced.leftToDo.toSet, equalTo(Set(A, B)))
    assertThat(reduced, instanceOf(classOf[Traverse]))
  }

  @Test def update_w_update() {
    val reduced = (update1 reduceWith update2).asInstanceOf[Update]
    assertThat(reduced.leftToDo.toSet, equalTo(Set(B, C)))
    assertThat(reduced, instanceOf(classOf[Update]))
    assertThat(reduced.cmds, equalTo(Seq(cmd1, cmd2)))
  }
}
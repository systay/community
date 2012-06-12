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

import org.neo4j.graphdb.{Lock, PropertyContainer}
import org.neo4j.cypher.RelatePathNotUnique

sealed abstract class RelateResult {
  def reduceWith(x: RelateResult): RelateResult = (this, x) match {

    case (Done(), Done()) => Done()
    case (Done(), other)  => other
    case (other, Done())  => other


    case (
      CanNotAdvance(todoA),
      CanNotAdvance(todoB))            => CanNotAdvance(todoA ++ todoB)
    case (CanNotAdvance(todoA), other) => other.addTodo(todoA)
    case (other, CanNotAdvance(todoA)) => other.addTodo(todoA)


    case (
      Traverse(leftToDoA, resultA),
      Traverse(leftToDoB, resultB))  => Traverse(leftToDoA ++ leftToDoB, resultA ++ resultB)
    case (traverse: Traverse, other) => traverse.addTodo(other.leftToDo)
    case (other, traverse: Traverse) => traverse.addTodo(other.leftToDo)


    case (
      Update(cmdsA, lockerA, leftToDoA),
      Update(cmdsB, lockerB, leftToDoB)) => Update(cmdsA ++ cmdsB, () => lockerA() ++ lockerB(), leftToDoA ++ leftToDoB)
  }


  def leftToDo: Seq[RelateLink]

  def addTodo(todo: Seq[RelateLink]): RelateResult
}

case class Done() extends RelateResult {
  def leftToDo = Seq()

  def addTodo(todo: Seq[RelateLink]): RelateResult = throw new Exception("Done can't add todos")
}

case class CanNotAdvance(leftToDo: Seq[RelateLink]) extends RelateResult {
  def addTodo(todo: Seq[RelateLink]): RelateResult = CanNotAdvance(leftToDo ++ todo)
}

case class Traverse(result: Seq[(String, PropertyContainer)], leftToDo: Seq[RelateLink]) extends RelateResult {
  assertValidResult()

  def addTodo(todo: Seq[RelateLink]): RelateResult = Traverse(result, leftToDo ++ todo)

  private def assertValidResult() {
    val uniqueKVPs = result.distinct
    val uniqueKeys = result.toMap

    if (uniqueKeys.size != uniqueKVPs.size) {
      //We can only go forward following a unique path. Fail.
      throw new RelatePathNotUnique("The pattern " + this + " produced multiple possible paths, and that is not allowed")
    }
  }
}

case class Update(cmds: Seq[UpdateWrapper], locker: () => Seq[Lock], leftToDo: Seq[RelateLink]) extends RelateResult {
  def lock(): Seq[Lock] = locker()

  def addTodo(todo: Seq[RelateLink]): RelateResult = Update(cmds, locker, leftToDo ++ todo)
}

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

import org.neo4j.graphdb.{Node, Relationship, Direction, RelationshipType}
import org.neo4j.cypher.internal.commands.{True, Predicate}
import collection.{mutable, Map}


trait ExpanderStep {
  def next: Option[ExpanderStep]
  def typ: Seq[RelationshipType]
  def direction: Direction
  def id: Int
  def nodePredicate: Predicate
  def createCopy(next:Option[ExpanderStep], direction:Direction, nodePredicate:Predicate):ExpanderStep
  def size:Int
  def expand(node: Node, parameters: Map[String, Any]): (Iterable[Relationship], Option[ExpanderStep])

  /*
  The way we reverse the steps is by first creating a Seq out of the steps. In this Seq, the first element points to
  Some(second), the second to Some(third), und so weiter, until the last element points to None.

  By doing a fold left and creating copies along the way, we reverse the directions - we push in None as what the first
  element will end up pointing to, and pass the steps to the next step. The result is that the first element points to
  None, the second to Some(first), und so weiter, until we pop out the last step as our reversed expander
   */
  def reverse(): ExpanderStep = {
    val allSteps = this.asSeq()

    val reversed = allSteps.foldLeft[Option[ExpanderStep]]((None)) {
      case (last, step) =>
        val p = step.next.map(_.nodePredicate).getOrElse(True())
        Some(step.createCopy(next = last, direction = step.direction.reverse(), nodePredicate = p))
    }

    assert(reversed.nonEmpty, "The reverse of an expander should never be empty")

    reversed.get
  }

  private def asSeq(): Seq[ExpanderStep] = {
    var allSteps = mutable.Seq[ExpanderStep]()
    var current: Option[ExpanderStep] = Some(this)

    while (current.nonEmpty) {
      val step = current.get
      allSteps = allSteps :+ step
      current = step.next
    }

    allSteps.toSeq
  }

}


class MiniMap(r: Relationship, n: Node, parameters: Map[String,Any]) extends Map[String, Any] {
  def get(key: String): Option[Any] =
    if (key == "r")
      Some(r)
    else if (key == "n")
      Some(n)
    else
      parameters.get(key)

  def iterator = throw new RuntimeException

  def -(key: String) = throw new RuntimeException

  def +[B1 >: Any](kv: (String, B1)) = throw new RuntimeException
}

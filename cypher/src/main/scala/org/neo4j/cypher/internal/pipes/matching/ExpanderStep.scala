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
import collection.mutable
import collection.JavaConverters._

case class ExpanderStep(id: Int, typ: Seq[RelationshipType], direction: Direction, next: Option[ExpanderStep]) {
  def pop() = next

  def reverse(): ExpanderStep = {
    val allSteps = getAllStepsAsSeq()

    val reversed = allSteps.foldLeft[Option[ExpanderStep]](None) {
      case (last, step) => Some(ExpanderStep(step.id, step.typ, step.direction.reverse(), last))
    }

    assert(reversed.nonEmpty, "The reverse of an expander should never be empty")

    reversed.get
  }

  def expand(node: Node): Iterable[Relationship] = typ match {
    case Seq() => node.getRelationships(direction).asScala
    case x     => node.getRelationships(direction, x: _*).asScala
  }

  private def getAllStepsAsSeq():Seq[ExpanderStep] = {
    var allSteps = mutable.Seq[ExpanderStep]()
    var current: Option[ExpanderStep] = Some(this)

    while (current.nonEmpty) {
      val step = current.get
      allSteps = allSteps :+ step
      current = step.next
    }

    allSteps.toSeq
  }

  private def shape = "(%s)%s-%s-%s".format(id, left, relInfo, right)

  private def left =
    if (direction == Direction.OUTGOING) ""
    else
      "<"

  private def right =
    if (direction == Direction.INCOMING) ""
    else
      ">"

  private def relInfo = typ.toList match {
    case List() => ""
    case _ => "[:%s]".format(typ.map(_.name()).mkString("|"))
  }

  override def toString = next match {
    case None    => shape + "()"
    case Some(x) => shape + x.toString
  }

  override def equals(p1: Any) = p1 match {
    case null                => false
    case other: ExpanderStep =>
      id == other.id &&
      direction == other.direction &&
      next == other.next &&
      typ.map(_.name()) == other.typ.map(_.name())
    case _                   => false
  }
}

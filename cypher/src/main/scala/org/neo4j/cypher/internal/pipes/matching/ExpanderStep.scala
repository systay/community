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

case class ExpanderStep(id:Int, typ: Seq[RelationshipType], direction: Direction, next: Option[ExpanderStep]) {
  def pop() = next

  def reverse(): ExpanderStep = {
    var l = mutable.Seq[ExpanderStep]()
    var current: Option[ExpanderStep] = Some(this)

    while (current.nonEmpty) {
      val step = current.get
      l = l :+ step
      current = step.next
    }

    val x: Option[ExpanderStep] = l.foldLeft[Option[ExpanderStep]](None) {
      case (last, step) => Some(ExpanderStep(step.id, step.typ, step.direction.reverse(), last))
    }

    assert(x.nonEmpty, "The reverse of an expander should never be empty")

    x.get
  }

  def expand(node: Node): Iterable[Relationship] = {
    val s = typ match {
      case Seq() => node.getRelationships(direction).asScala
      case x     => node.getRelationships(direction, x: _*).asScala
    }
    s
  }
}

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
import org.neo4j.cypher.internal.commands.Predicate
import collection.JavaConverters._
import org.neo4j.cypher.internal.pipes.ExecutionContext

case class VarLengthStep(id: Int,
                         typ: Seq[RelationshipType],
                         direction: Direction,
                         min: Int,
                         max: Option[Int],
                         next: Option[ExpanderStep],
                         relPredicate: Predicate,
                         nodePredicate: Predicate) extends ExpanderStep {
  def createCopy(next: Option[ExpanderStep], direction: Direction, nodePredicate: Predicate): ExpanderStep =
    copy(next = next, direction = direction, nodePredicate = nodePredicate)

  def expand(node: Node, parameters: ExecutionContext):  (Iterable[Relationship], Option[ExpanderStep]) = {
    def filter(r: Relationship, n: Node): Boolean = {
      val m = new MiniMap(r, n, parameters)
      relPredicate.isMatch(m) && nodePredicate.isMatch(m)
    }

    def decrease(v: Option[Int]): Option[Int] = v.map {
      case 0 => 0
      case x => x - 1
    }

    def forceNextStep() = next match {
      case None       => (Seq(), None)
      case Some(step) => step.expand(node, parameters)
    }

    def expandeRecursively(rels: Iterable[Relationship]): Iterable[Relationship] = {
      if (min == 0) {
        rels ++ next.map(s => s.expand(node, parameters)._1).flatten
      } else {
        rels
      }
    }

    def decreaseAndReturnNewNextStep: Option[ExpanderStep] = {
      if (max == Some(1)) {
        next
      } else {
        val newMax = decrease(max)
        Some(copy(min = if (min == 0) 0 else min - 1, max = newMax))
      }
    }

    val foundRelationships = typ match {
      case Seq() => node.getRelationships(direction).asScala.filter(r => filter(r, r.getOtherNode(node)))
      case x     => node.getRelationships(direction, x: _*).asScala.filter(r => filter(r, r.getOtherNode(node)))
    }

    if (foundRelationships.isEmpty) {
      forceNextStep()
    } else {
      (expandeRecursively(foundRelationships), decreaseAndReturnNewNextStep)
    }
  }

  def size: Int = 0

  override def toString = {
    def predicateString = "r: %s, n: %s".format(relPredicate, nodePredicate)

    def shape = "(%s)%s-[:%s*%s {%s}]-%s".format(id, left, typeString, varLengthString, predicateString, right)

    def left =
      if (direction == Direction.OUTGOING)
        ""
      else
        "<"

    def right =
      if (direction == Direction.INCOMING)
        ""
      else
        ">"

    def typeString =
      typ.map(_.name()).mkString("|")

    def varLengthString = max match {
      case None    => "%s..".format(min)
      case Some(y) => "%s..%s".format(min, y)
    }

    next match {
      case None    => "%s()".format(shape)
      case Some(x) => shape + x.toString
    }
  }
}
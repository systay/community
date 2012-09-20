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

import org.neo4j.graphdb.{Direction, PropertyContainer}
import org.neo4j.cypher.internal.symbols.{RelationshipType, NodeType, SymbolTable}
import org.neo4j.cypher.internal.pipes.matching.ExpanderStep
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.internal.commands.{Predicate, RelatedTo, True}


object TrailBuilder {
  def findLongestTrail(patterns: Seq[RelatedTo], boundPoints: Seq[String], predicates: Seq[Predicate] = Seq.empty) =
    new TrailBuilder(patterns, boundPoints, predicates).findLongestTrail()
}

final case class LongestTrail(start: String, end: Option[String], remainingPattern: Seq[RelatedTo], longestTrail: Trail) {
  lazy val step = longestTrail.toSteps(0).get.reverse()
}

final class TrailBuilder(patterns: Seq[RelatedTo], boundPoints: Seq[String], predicates: Seq[Predicate]) {


  private def internalFindLongestPath(doneSeq: Seq[(Trail, Seq[RelatedTo])]): Seq[(Trail, Seq[RelatedTo])] = {
    val result: Seq[(Trail, Seq[RelatedTo])] = doneSeq.flatMap {
      case (done: Trail, patterns: Seq[RelatedTo]) =>
        val relatedToes = patterns.filter {
          rel => done.end == rel.left || done.end == rel.right
        }

        if (relatedToes.isEmpty)
          Seq((done, patterns))
        else {
          Seq((done, patterns)) ++
            relatedToes.map {
              case rel if rel.left == done.end => (WrappingTrail(done, rel.direction.reverse(), rel.relName, rel.relTypes, rel.right, predicates), patterns.filterNot(_ == rel))
              case rel                         => (WrappingTrail(done, rel.direction, rel.relName, rel.relTypes, rel.left, predicates), patterns.filterNot(_ == rel))
            }
        }
    }

    if (result.distinct == doneSeq.distinct)
      result
    else
      internalFindLongestPath(result)
  }

  def findLongestTrail(): Option[LongestTrail] =
    if (patterns.isEmpty) None
    else {
      val foundPaths: Seq[(Trail, Seq[RelatedTo])] =
        internalFindLongestPath(boundPoints.map {
          point => (BoundPoint(point), patterns)
        }).
          filter {
          case (trail, toes) => trail.size > 0 && trail.start != trail.end
        }

      val pathsBetweenBoundPoints: Seq[(Trail, Seq[RelatedTo])] = findCompatiblePaths(foundPaths)

      if (pathsBetweenBoundPoints.isEmpty)
        None
      else {
        val almost = pathsBetweenBoundPoints.sortBy(_._1.size)
        val (longestPath, remainingPattern) = almost.last

        val start = longestPath.start
        val end = if (boundPoints.contains(longestPath.end)) Some(longestPath.end) else None


        if (longestPath.size < 2)
          None
        else
          Some(LongestTrail(start, end, remainingPattern, longestPath))
      }
    }

  def findCompatiblePaths(foundPaths: Seq[(Trail, Seq[RelatedTo])]): Seq[(Trail, Seq[RelatedTo])] = {
    val boundInTwoPoints = foundPaths.filter {
      case (p, left) => boundPoints.contains(p.start) && boundPoints.contains(p.end)
    }

    if (boundInTwoPoints.nonEmpty)
      boundInTwoPoints
    else
      Seq.empty
    /* FIXME
    foundPaths.filter {
      case (p, left) => boundPoints.contains(p.start) || boundPoints.contains(p.end)
    }
    */
  }
}

sealed abstract class Trail {
  def pathDescription: Seq[String]

  def start: String

  def end: String

  def size: Int

  def toSteps(id: Int): Option[ExpanderStep]

  override def toString: String = pathDescription.toString()

  def decompose(p: Seq[PropertyContainer]): Map[String, Any] = decompose(p, Map.empty)._2

  protected[builders] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]): (Seq[PropertyContainer], Map[String, Any])

  def symbols(table: SymbolTable): SymbolTable

  def contains(target: String): Boolean

  def predicates:Seq[Predicate]
}

final case class BoundPoint(name: String) extends Trail {
  def end = name

  def pathDescription = Seq(name)

  def start = name

  def size = 0

  def toSteps(id: Int) = None

  protected[builders] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]) = {
    assert(p.size == 1, "Expected a path with a single node in it")
    (p.tail, r ++ Map(name -> p.head))
  }

  def symbols(table: SymbolTable): SymbolTable = table.add(name, NodeType())

  def contains(target: String): Boolean = target == name

  def predicates = Seq.empty
}

final case class WrappingTrail(s: Trail,
                               dir: Direction,
                               rel: String,
                               typ: Seq[String],
                               end: String,
                               candPredicates: Seq[Predicate]) extends Trail {

  val relPred  = candPredicates.find(createFinder(rel))
  val nodePred = candPredicates.find(createFinder(end))

  private def containsSingle(set: Set[String], elem: String) = set.size == 1 && set.head == elem

  private def createFinder(elem: String): (Predicate => Boolean) =
    (pred: Predicate) => containsSingle(pred.symbolTableDependencies, elem)

  def start = s.start

  def pathDescription = s.pathDescription ++ Seq(rel, end)

  def toSteps(id: Int) = {
    val types = typ.map(withName(_))
    val steps = s.toSteps(id + 1)
    val relPredicate = relPred.getOrElse(True())
    val nodePredicate = nodePred.getOrElse(True())

    Some(ExpanderStep(id, types, dir, steps, relPredicate, nodePredicate))
  }

  def size = s.size + 1

  protected[builders] def decompose(p: Seq[PropertyContainer], m: Map[String, Any]) = {
    val r = p.tail.head
    val n = p.head
    val newMap = m + (rel -> r) + (end -> n)
    s.decompose(p.tail.tail, newMap)
  }

  def symbols(table: SymbolTable): SymbolTable = s.symbols(table).add(end, NodeType()).add(rel, RelationshipType())

  def contains(target: String): Boolean = s.contains(target) || target == end

  def predicates = nodePred.toSeq ++ relPred.toSeq ++ s.predicates
}

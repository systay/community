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

import org.neo4j.cypher.internal.commands.{VarLengthRelatedTo, RelatedTo, Pattern, Predicate}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.expressions.{Identifier, Property}
import org.neo4j.cypher.internal.pipes.matching.{BoundPoint, VariableLengthStepTrail, SingleStepTrail, Trail}
import org.neo4j.helpers.ThisShouldNotHappenError
import annotation.tailrec

object TrailBuilder {
  def findLongestTrail(patterns: Seq[Pattern], boundPoints: Seq[String], predicates: Seq[Predicate] = Seq.empty) =
    new TrailBuilder(patterns, boundPoints, predicates).findLongestTrail()
}

final case class LongestTrail(start: String, end: Option[String], longestTrail: Trail) {
  lazy val step = longestTrail.toSteps(0).get
}

final class TrailBuilder(patterns: Seq[Pattern], boundPoints: Seq[String], predicates: Seq[Predicate]) {
  @tailrec
  private def internalFindLongestPath(doneSeq: Seq[(Trail, Seq[Pattern])]): Seq[(Trail, Seq[Pattern])] = {

    def createFinder(elem: String): (Predicate => Boolean) = {
      def containsSingle(set: Set[String]) = set.size == 1 && set.head == elem
      (pred: Predicate) => containsSingle(pred.symbolTableDependencies)
    }

    def transformToTrail(p: Pattern, done: Trail): (Trail, Seq[Pattern]) = {
      def predicateRewriter(from: String, to: String)(pred: Predicate) = pred.rewrite {
        case Identifier(name) if name == from     => Identifier(to)
        case Property(name, prop) if name == from => Property(to, prop)
        case e                                    => e
      }
      def relPred(k: String) = predicates.find(createFinder(k)).map(predicateRewriter(k, "r"))
      def nodePred(k: String) = predicates.find(createFinder(k)).map(predicateRewriter(k, "n"))
      def singleStep(rel: RelatedTo, end: String, dir: Direction) = done.add(start => SingleStepTrail(BoundPoint(end), dir, rel.relName, rel.relTypes, start, relPred(rel.relName), nodePred(start), rel))
      def multiStep(rel: VarLengthRelatedTo, end: String, dir: Direction) = done.add(start => VariableLengthStepTrail(BoundPoint(end), dir, rel.relTypes, rel.minHops.getOrElse(1), rel.maxHops, rel.pathName, rel.relIterator, start, rel))

      val patternsLeft = patterns.filterNot(_ == p)

      val result: Trail = p match {
        case rel: RelatedTo if rel.left == done.end           => singleStep(rel, rel.right, rel.direction)
        case rel: RelatedTo if rel.right == done.end          => singleStep(rel, rel.left, rel.direction.reverse())
        case rel: VarLengthRelatedTo if rel.end == done.end   => multiStep(rel, rel.start, rel.direction.reverse())
        case rel: VarLengthRelatedTo if rel.start == done.end => multiStep(rel, rel.end, rel.direction)
        case _                                                => throw new ThisShouldNotHappenError("Andres", "Wut?")
      }

      (result, patternsLeft)
    }

    val result: Seq[(Trail, Seq[Pattern])] = doneSeq.flatMap {
      case (done: Trail, patterns: Seq[Pattern]) =>
        val relatedToes: Seq[Pattern] = patterns.filter {
          case rel: RelatedTo          => done.end == rel.left || done.end == rel.right
          case rel: VarLengthRelatedTo => done.end == rel.end || done.end == rel.start
        }

        val newValues = relatedToes.map(transformToTrail(_, done))
        Seq((done, patterns)) ++ newValues
    }

    if (result.distinct == doneSeq.distinct)
      result
    else
      internalFindLongestPath(result)
  }

  private def findLongestTrail(): Option[LongestTrail] = {
    def findAllPaths(): Seq[(Trail, scala.Seq[Pattern])] = {
      val startPoints = boundPoints.map(point => (BoundPoint(point), patterns))
      val foundPaths = internalFindLongestPath(startPoints)
      val filteredPaths = foundPaths.filter {
        case (trail, toes) => trail.size > 0 && trail.start != trail.end
      }
      filteredPaths
    }


    if (patterns.isEmpty) {
      None
    }
    else {
      val foundPaths: Seq[(Trail, Seq[Pattern])] = findAllPaths()
      val pathsBetweenBoundPoints: Seq[(Trail, Seq[Pattern])] = findCompatiblePaths(foundPaths)

      if (pathsBetweenBoundPoints.isEmpty) {
        None
      } else {
        val trail = findLongestTrail(pathsBetweenBoundPoints)

        Some(trail)
      }
    }
  }

  private def findLongestTrail(pathsBetweenBoundPoints: scala.Seq[(Trail, scala.Seq[Pattern])]): LongestTrail = {
    val almost = pathsBetweenBoundPoints.sortWith {
      case ((t1,_), (t2,_)) => t1.size<t2.size || t1.start>t2.start //Sort first by length, and then by start point
    }

    val (longestPath, _) = almost.last

    val start = longestPath.start
    val end = if (boundPoints.contains(longestPath.end)) Some(longestPath.end) else None
    LongestTrail(start, end, longestPath)
  }


  private def findCompatiblePaths(incomingPaths: Seq[(Trail, Seq[Pattern])]): Seq[(Trail, Seq[Pattern])] = {
    val pathsWithoutBoundPointsInMiddle = incomingPaths.filterNot {
      case (trail, _) => hasBoundPointsInMiddleOfPath(trail)
    }


    val boundInBothEnds = pathsWithoutBoundPointsInMiddle.filter {
      case (p, left) => boundPoints.contains(p.start) && boundPoints.contains(p.end)
    }

    val boundInOneEnd = pathsWithoutBoundPointsInMiddle.filter {
      case (p, left) => boundPoints.contains(p.start) || boundPoints.contains(p.end)
    }

    if (boundInBothEnds.nonEmpty)
      boundInBothEnds
    else
      boundInOneEnd
  }

  def hasBoundPointsInMiddleOfPath(trail: Trail): Boolean = {
    val nodesInBetween = trail.nodeNames.toSet -- Set(trail.start, trail.end)

    nodesInBetween exists(boundPoints.contains)
  }
}
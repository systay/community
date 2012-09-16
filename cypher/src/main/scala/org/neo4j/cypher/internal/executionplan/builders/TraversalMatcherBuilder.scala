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

import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.commands.{NodeByIndex, NodeByIndexQuery, RelatedTo}
import org.neo4j.cypher.internal.pipes.matching.ExpanderStep
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher.internal.executionplan.builders.TraversalMatcherBuilder.LongestPathResult

class TraversalMatcherBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = null

  def canWorkWith(plan: ExecutionPlanInProgress) = extractExpanderStepsFromQuery(plan).isEmpty

  private def extractExpanderStepsFromQuery(plan: ExecutionPlanInProgress): Option[LongestPathResult] = {
    val startPoints = plan.query.start.flatMap {
      case Unsolved(NodeByIndexQuery(id, _, _)) => Some(id)
      case Unsolved(NodeByIndex(id, _, _, _))   => Some(id)
      case _                                    => None
    }

    val pattern = plan.query.patterns.flatMap {
      case Unsolved(r: RelatedTo) => Some(r)
      case _                      => None
    }

    TraversalMatcherBuilder.findLongestPath(pattern, startPoints)
  }

  def priority = PlanBuilder.TraversalMatcher
}

object TraversalMatcherBuilder {

  abstract class Path {
    def pathDescription: Seq[String]
    def start: String
    def end: String
    def size: Int
    def toSteps(id: Int): Option[ExpanderStep]
    def relNames: Seq[String]
    override def toString: String = pathDescription.toString()
  }

  case class BoundPoint(name: String) extends Path {
    def end = name
    def pathDescription = Seq(name)
    def start = name
    def size = 0
    def relNames = Seq()
    def toSteps(id: Int) = None
  }

  case class WrappingPath(s: Path, dir: Direction, rel: String, typ: Seq[String], end: String) extends Path {
    def relNames = s.relNames :+ rel
    def start = s.start
    def pathDescription = s.pathDescription ++ Seq(rel, end)
    def toSteps(id: Int) = Some(ExpanderStep(id, typ.map(withName(_)), dir, s.toSteps(id + 1)))
    def size = s.size + 1
  }

  private def internalFindLongestPath(doneSeq: Seq[(Path, Seq[RelatedTo])]): Seq[(Path, Seq[RelatedTo])] = {
    val result: Seq[(Path, Seq[RelatedTo])] = doneSeq.flatMap {
      case (done: Path, patterns: Seq[RelatedTo]) =>
        val relatedToes = patterns.filter(rel => done.end == rel.left || done.end == rel.right)

        if (relatedToes.isEmpty)
          Seq((done, patterns))
        else {
          relatedToes.map {
            case rel if rel.left == done.end => (WrappingPath(done, rel.direction.reverse(), rel.relName, rel.relTypes, rel.right), patterns.filterNot(_==rel))
            case rel                         => (WrappingPath(done, rel.direction, rel.relName, rel.relTypes, rel.left), patterns.filterNot(_==rel))
          }
        }
    }

    if (result == doneSeq)
      result
    else
      internalFindLongestPath(result)
  }

  case class LongestPathResult(step:ExpanderStep, start:String, end:Option[String], remainingPattern:Seq[RelatedTo])

  def findLongestPath(patterns: Seq[RelatedTo], boundPoints: Seq[String]): Option[LongestPathResult] = {
    val foundPaths: Seq[(Path, Seq[RelatedTo])] = internalFindLongestPath(boundPoints.map(point => (BoundPoint(point), patterns)))

    val pathsBetweenBoundPoints: Seq[(Path, Seq[RelatedTo])] = findCompatiblePaths(foundPaths, boundPoints)

    if (pathsBetweenBoundPoints.isEmpty)
      None
    else {
      val almost = pathsBetweenBoundPoints.sortBy(_._1.size)
      val (longestPath,remainingPattern) = almost.last

      val start = longestPath.start
      val end = if (boundPoints.contains(longestPath.end)) Some(longestPath.end) else None

      val longestPathSteps = longestPath.toSteps(0).get.reverse()
      Some(LongestPathResult(longestPathSteps, start, end, remainingPattern))
    }
  }

  def findCompatiblePaths(foundPaths: Seq[(Path, Seq[RelatedTo])], boundPoints: scala.Seq[String]): Seq[(Path, Seq[RelatedTo])] = {
    val boundInTwoPoints = foundPaths.filter {
      case (p, left) => boundPoints.contains(p.start) && boundPoints.contains(p.end)
    }

    if (boundInTwoPoints.nonEmpty)
      boundInTwoPoints
    else
      foundPaths.filter {
        case (p, left) => boundPoints.contains(p.start) || boundPoints.contains(p.end)
      }
  }
}
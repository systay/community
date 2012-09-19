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

import org.neo4j.cypher.internal.executionplan.PlanBuilder
import org.neo4j.cypher.internal.commands._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb
import graphdb.{Node, GraphDatabaseService, Direction, PropertyContainer}
import org.neo4j.cypher.internal.pipes.{TraversalMatchPipe, ExecutionContext}
import org.neo4j.cypher.internal.executionplan.builders.TraversalMatcherBuilder.LongestPathResult
import org.neo4j.cypher.internal.pipes.matching.{UnidirectionalTraversalMatcher, BidirectionalTraversalMatcher, ExpanderStep}
import org.neo4j.cypher.internal.executionplan.ExecutionPlanInProgress
import scala.Some
import org.neo4j.cypher.internal.commands.NodeByIndex
import org.neo4j.cypher.internal.commands.NodeByIndexQuery
import org.neo4j.cypher.internal.symbols.{RelationshipType, NodeType, SymbolTable}

class TraversalMatcherBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress): ExecutionPlanInProgress = extractExpanderStepsFromQuery(plan) match {
    case None              => throw new ThisShouldNotHappenError("Andres", "This plan should not have been accepted")
    case Some(longestPath) =>
      val LongestPathResult(start, end, remainingPattern, longestTrail) = longestPath

      val markedPatterns = plan.query.patterns.map {
        case Unsolved(p: RelatedTo) if remainingPattern.contains(p) => Unsolved[Pattern](p)
        case Unsolved(p: RelatedTo)                                 => Solved[Pattern](p)
        case x                                                      => x
      }

      val unsolvedItems = plan.query.start.filter(_.unsolved)
      val (startToken, startNodeFn) = identifier2nodeFn(graph, start, unsolvedItems)


      val (matcher,tokens) = if (end.isEmpty) {
        val matcher = new UnidirectionalTraversalMatcher(longestPath.step, startNodeFn)
        (matcher, Seq(startToken))
      } else {
        val (endToken, endNodeFn) = identifier2nodeFn(graph, end.get, unsolvedItems)
        val matcher = new BidirectionalTraversalMatcher(longestPath.step, startNodeFn, endNodeFn)
        (matcher, Seq(startToken, endToken))
      }

      val newQ = plan.query.copy(
        patterns = markedPatterns,
        start = plan.query.start.filterNot(tokens.contains) ++ tokens.map(_.solve)
      )

      val pipe = new TraversalMatchPipe(plan.pipe, matcher, longestTrail)

      plan.copy(pipe = pipe, query = newQ)
  }

  def identifier2nodeFn(graph: GraphDatabaseService, identifier: String, unsolvedItems: Seq[QueryToken[StartItem]]):
  (QueryToken[StartItem], (ExecutionContext) => Iterable[Node]) = {
    val token = unsolvedItems.filter { (item) => identifier == item.token.identifierName }.head
    (token, IndexQueryBuilder.getNodeGetter(token.token, graph))
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val steps = extractExpanderStepsFromQuery(plan)
    steps.nonEmpty
  }

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

  abstract class Trail {
    def pathDescription: Seq[String]
    def start: String
    def end: String
    def size: Int
    def toSteps(id: Int): Option[ExpanderStep]
    override def toString: String = pathDescription.toString()

    def decompose(p: Seq[PropertyContainer]): Map[String, Any] = decompose(p, Map.empty)._2
    protected[TraversalMatcherBuilder] def decompose(p:Seq[PropertyContainer], r:Map[String,Any]):(Seq[PropertyContainer], Map[String,Any])

    def symbols(table: SymbolTable): SymbolTable

    def contains(target: String): Boolean
  }

  case class BoundPoint(name: String) extends Trail {
    def end = name
    def pathDescription = Seq(name)
    def start = name
    def size = 0
    def toSteps(id: Int) = None
    protected[TraversalMatcherBuilder] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]) = {
      assert(p.size == 1, "Expected a path with a single node in it")
      (p.tail, r ++ Map(name -> p.head))
    }

    def symbols(table: SymbolTable): SymbolTable = table.add(name, NodeType())

    def contains(target: String): Boolean = target == name
  }

  case class WrappingTrail(s: Trail, dir: Direction, rel: String, typ: Seq[String], end: String) extends Trail {
    def start = s.start
    def pathDescription = s.pathDescription ++ Seq(rel, end)
    def toSteps(id: Int) = Some(ExpanderStep(id, typ.map(withName(_)), dir, s.toSteps(id + 1), True(), True()))
    def size = s.size + 1
    protected[TraversalMatcherBuilder] def decompose(p: Seq[PropertyContainer], m: Map[String, Any]) = {
      val r = p.tail.head
      val n = p.head
      val newMap = m + (rel->r) + (end->n)
      s.decompose(p.tail.tail, newMap)
    }

    def symbols(table: SymbolTable): SymbolTable = s.symbols(table).add(end, NodeType()).add(rel, RelationshipType())

    def contains(target: String): Boolean = s.contains(target) || target == end
  }

  private def internalFindLongestPath(doneSeq: Seq[(Trail, Seq[RelatedTo])]): Seq[(Trail, Seq[RelatedTo])] = {
    val result: Seq[(Trail, Seq[RelatedTo])] = doneSeq.flatMap {
      case (done: Trail, patterns: Seq[RelatedTo]) =>
        val relatedToes = patterns.filter { rel => done.end == rel.left || done.end == rel.right }

        if (relatedToes.isEmpty)
          Seq((done, patterns))
        else {
          Seq((done, patterns)) ++
          relatedToes.map {
            case rel if rel.left == done.end => (WrappingTrail(done, rel.direction.reverse(), rel.relName, rel.relTypes, rel.right), patterns.filterNot(_ == rel))
            case rel                         => (WrappingTrail(done, rel.direction, rel.relName, rel.relTypes, rel.left), patterns.filterNot(_ == rel))
          }
        }
    }

    if (result.distinct == doneSeq.distinct)
      result
    else
      internalFindLongestPath(result)
  }

  case class LongestPathResult(start: String, end: Option[String], remainingPattern: Seq[RelatedTo], longestPath: Trail) {
    lazy val step = longestPath.toSteps(0).get.reverse()
  }

  def findLongestPath(patterns: Seq[RelatedTo], boundPoints: Seq[String]): Option[LongestPathResult] = if (patterns.isEmpty) None else {
    val foundPaths: Seq[(Trail, Seq[RelatedTo])] =
      internalFindLongestPath(boundPoints.map { point => (BoundPoint(point), patterns) }).
      filter { case (trail, toes) => trail.size > 0 && trail.start != trail.end }

    val pathsBetweenBoundPoints: Seq[(Trail, Seq[RelatedTo])] = findCompatiblePaths(foundPaths, boundPoints)

    if (pathsBetweenBoundPoints.isEmpty)
      None
    else {
      val almost = pathsBetweenBoundPoints.sortBy(_._1.size)
      val (longestPath, remainingPattern) = almost.last

      val start = longestPath.start
      val end = if (boundPoints.contains(longestPath.end)) Some(longestPath.end) else None

      Some(LongestPathResult(start, end, remainingPattern, longestPath))
    }
  }

  def findCompatiblePaths(foundPaths: Seq[(Trail, Seq[RelatedTo])], boundPoints: scala.Seq[String]): Seq[(Trail, Seq[RelatedTo])] = {
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
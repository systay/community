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

import org.neo4j.cypher.internal.symbols.{MapType, SymbolTable}
import collection.mutable.{Set => MutableSet}
import org.neo4j.cypher.{PatternException, SyntaxException}
import org.neo4j.helpers.ThisShouldNotHappenError

class PatternGraph(val patternNodes: Map[String, PatternNode],
                   val patternRels: Map[String, PatternRelationship],
                   val bindings: SymbolTable) {

  val (patternGraph, optionalElements, containsLoops, doubleOptionalPaths) = validatePattern(patternNodes, patternRels, bindings)

  def apply(key: String) = patternGraph(key)

  val hasDoubleOptionals: Boolean = doubleOptionalPaths.nonEmpty

  def get(key: String) = patternGraph.get(key)

  def contains(key: String) = patternGraph.contains(key)

  def keySet = patternGraph.keySet

  def containsOptionalElements = optionalElements.nonEmpty

  def boundElements = bindings.identifiers.filter(id => MapType().isAssignableFrom(id.typ)).map(_.name)

  private def validatePattern(patternNodes: Map[String, PatternNode],
                              patternRels: Map[String, PatternRelationship],
                              bindings: SymbolTable): (Map[String, PatternElement], Set[String], Boolean, Seq[DoubleOptionalPath]) = {
    val overlaps = patternNodes.keys.filter(patternRels.keys.toSeq contains)
    if (overlaps.nonEmpty) {
      throw new PatternException("Some identifiers are used as both relationships and nodes: " + overlaps.mkString(", "))
    }

    val elementsMap: Map[String, PatternElement] = (patternNodes.values ++ patternRels.values).map(x => (x.key -> x)).toMap
    val allElements = elementsMap.values.toSeq

    val boundElements = bindings.identifiers.flatMap(i => elementsMap.get(i.name))

    val hasLoops = checkIfWeHaveLoops(boundElements, allElements)
    val optionalSet = getOptionalElements(boundElements, allElements)
    val doubleOptionals = getDoubleOptionals(boundElements)

    (elementsMap, optionalSet, hasLoops, doubleOptionals)
  }

  private def reduceDoubleOptionals(dops: Seq[DoubleOptionalPath]) = {
    dops.distinct

  }

  private def getDoubleOptionals(boundPatternElements: Seq[PatternElement]): Seq[DoubleOptionalPath] = {
    var doubleOptionals = Seq[DoubleOptionalPath]()


    boundPatternElements.foreach(e => e.traverse(
      shouldFollow = e => true,
      visit = (e, data: Seq[PatternElement]) => {
        val result = data :+ e

        val foundPathBetweenBoundElements = boundPatternElements.contains(e) && result.size > 1

        if (foundPathBetweenBoundElements) {
          val numberOfOptionals = result.foldLeft(Seq[String]())((count, element) => {
            val r = element match {
              case x: PatternRelationship if x.optional => Some(x.key)
              case _                                    => None
            }

            count ++ r
          })

          if (numberOfOptionals.size > 2) {
            throw new PatternException("Your pattern has at least one path between two bound elements, and these patterns are undefined for the time being. Valid use cases for this are very interesting to us - let us know at cypher@neo4j.org")
          }

          if (numberOfOptionals.size == 2) {

            val leftNode = data(0)
            val leftRel = numberOfOptionals(0)
            val rightNode = e
            val rightRel = numberOfOptionals(1)

            doubleOptionals = doubleOptionals :+ DoubleOptionalPath.create(leftNode.key, rightNode.key, leftRel, rightRel)
          }
        }


        result
      },
      data = Seq[PatternElement](),
      path = Seq()
    ))

    reduceDoubleOptionals(doubleOptionals)
  }

  private def getOptionalElements(boundPatternElements: Seq[PatternElement], allPatternElements: Seq[PatternElement]): Set[String] = {
    val optionalElements = MutableSet[String](allPatternElements.map(_.key): _*)
    var visited = Set[PatternElement]()

    boundPatternElements.foreach(n => n.traverse(
      shouldFollow = e => {
        e match {
          case x: PatternNode         => !visited.contains(e)
          case x: PatternRelationship => !visited.contains(e) && !x.optional
        }
      },
      visit = (e, x: Unit) => {
        optionalElements.remove(e.key)
        visited = visited ++ Set(e)
      },
      data = (),
      path = Seq()
    ))

    optionalElements.toSet
  }

  private def checkIfWeHaveLoops(boundPatternElements: Seq[PatternElement], allPatternElements: Seq[PatternElement]) = {
    var visited = Seq[PatternElement]()
    var loop = false

    val follow = (element: PatternElement) => element match {
      case n: PatternNode         => true
      case r: PatternRelationship => !visited.contains(r)
    }

    val vNode = (n: PatternNode, x: Unit) => {
      if (visited.contains(n))
        loop = true
      visited = visited :+ n
    }

    val vRel = (r: PatternRelationship, x: Unit) => visited :+= r

    boundPatternElements.foreach {
      case pr: PatternRelationship => pr.startNode.traverse(follow, vNode, vRel, (), Seq())
      case pn: PatternNode         => pn.traverse(follow, vNode, vRel, (), Seq())
    }

    val notVisitedElements = allPatternElements.filterNot(visited contains)
    if (notVisitedElements.nonEmpty) {
      throw new SyntaxException("All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: " + notVisitedElements.map(_.key).mkString("", ", ", ""))
    }

    loop
  }
}


object DoubleOptionalPath {
  def create(n1: String, n2: String, r1: String, r2: String) = {
    val (startNode, endNode, startRel, endRel) = if (n1 < n2)
      (n1, n2, r1, r2)
    else
      (n2, n1, r2, r1)

    new DoubleOptionalPath(startNode, endNode, Seq(startRel), Seq(endRel))
  }
}

case class DoubleOptionalPath(startNode: String, endNode: String, rel1: Seq[String], rel2: Seq[String]) {
  def canRun(s: String): Boolean = startNode == s || endNode == s

  def reduceWith(other:DoubleOptionalPath):DoubleOptionalPath= {
    require(other.startNode == startNode)
    require(other.endNode == endNode)

    new DoubleOptionalPath(startNode, endNode, (rel1 ++ other.rel1).distinct, (rel2 ++ other.rel2).distinct)
  }

  def otherNode(nodeName: String) = nodeName match {
    case x if x == startNode => endNode
    case x if x == endNode   => startNode
  }

  def otherRel(nodeName: String) = nodeName match {
    case x if x == startNode => rel2
    case x if x == endNode   => rel1
  }

  def shouldDoWork(current: String, remaining: Set[MatchingPair]): Boolean = {
    val fromLeft = startNode == current && remaining.exists(_.patternNode.key == endNode)
    val fromRight = endNode == current && remaining.exists(_.patternNode.key == startNode)
    fromLeft || fromRight
  }
}
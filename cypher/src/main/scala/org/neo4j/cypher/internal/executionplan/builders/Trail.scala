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
import org.neo4j.cypher.internal.commands.{Pattern, True, Predicate}
import org.neo4j.graphdb.DynamicRelationshipType._
import org.neo4j.cypher.internal.pipes.matching.{VarLengthStep, SingleStep, ExpanderStep}

sealed abstract class Trail {
  def pathDescription: Seq[String]
  def start: String
  def end: String
  def size: Int
  def toSteps(id: Int): Option[ExpanderStep]
  override def toString: String = pathDescription.toString()
  def decompose(p: Seq[PropertyContainer]): Seq[Map[String, Any]] = decompose(p, Map.empty).map(_._2)

  protected[builders] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]): Seq[(Seq[PropertyContainer], Map[String, Any])]

  def symbols(table: SymbolTable): SymbolTable
  def contains(target: String): Boolean
  def predicates: Seq[Predicate]
  def patterns: Seq[Pattern]
}

final case class BoundPoint(name: String) extends Trail {
  def end = name
  def pathDescription = Seq(name)
  def start = name
  def size = 0
  def toSteps(id: Int) = None
  protected[builders] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]) = {
    assert(p.size == 1, "Expected a path with a single node in it")
    Seq((p.tail, r ++ Map(name -> p.head)))
  }

  def symbols(table: SymbolTable): SymbolTable = table.add(name, NodeType())
  def contains(target: String): Boolean = target == name
  def predicates = Seq.empty
  def patterns = Seq.empty
}

final case class SingleStepTrail(s: Trail,
                                 dir: Direction,
                                 rel: String,
                                 typ: Seq[String],
                                 end: String,
                                 relPred: Option[Predicate],
                                 nodePred: Option[Predicate],
                                 pattern: Pattern) extends Trail {
  def start = s.start

  def pathDescription = s.pathDescription ++ Seq(rel, end)

  def toSteps(id: Int) = {
    val types = typ.map(withName(_))
    val steps = s.toSteps(id + 1)
    val relPredicate = relPred.getOrElse(True())
    val nodePredicate = nodePred.getOrElse(True())

    Some(SingleStep(id, types, dir, steps, relPredicate, nodePredicate))
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

  def patterns = s.patterns :+ pattern
}

final case class VariableLengthStepTrail(s: Trail,
                                         dir: Direction,
                                         typ: Seq[String],
                                         min: Int,
                                         max: Option[Int],
                                         path: String,
                                         relIterator: Option[String],
                                         end: String,
                                         pattern: Pattern) extends Trail {
  def contains(target: String) = false

  protected[builders] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]) = null

  def pathDescription = s.pathDescription ++ Seq(path, end) ++ relIterator

  def patterns = null

  def predicates = null

  def size = s.size + 1

  def start = s.start

  def symbols(table: SymbolTable) = null

  def toSteps(id: Int): Option[ExpanderStep] = {
    val types = typ.map(withName(_))
    val steps = s.toSteps(id + 1)

    Some(VarLengthStep(id, types, dir, min, max, steps, True(), True()))
  }
}
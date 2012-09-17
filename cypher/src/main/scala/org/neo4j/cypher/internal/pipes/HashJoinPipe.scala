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
package org.neo4j.cypher.internal.pipes


import collection.mutable

class HashJoinPipe(a: Pipe, b: Pipe) extends Pipe {
  val keySet = a.symbols.identifiers.keySet.intersect(b.symbols.identifiers.keySet)
  assert(keySet.nonEmpty, "No overlap between the incoming pipes exist")

  val keySeq = keySet.toSeq

  def createResults(state: QueryState): Traversable[ExecutionContext] = {
    val table = buildTable(a.createResults(state))

    b.createResults(state).flatMap { (entry) =>
      table.get(computeKey(entry)) match {
        case Some(aList) => aList.map { _.newWith(entry) }
        case None        => Seq.empty
      }
    }
  }

  protected def buildTable(iter: scala.Traversable[ExecutionContext]):
  mutable.HashMap[Seq[Any], List[ExecutionContext]] = {
    val table = mutable.HashMap[Seq[Any], List[ExecutionContext]]()
    for (entry <- iter) {
      val key = computeKey(entry)
      val l = table.getOrElse(key, List())
      table(key) = entry :: l
    }
    table
  }

  def computeKey(m: mutable.Map[String, Any]): Seq[Any] = keySeq.map { m(_) }
  def symbols = a.symbols.add(b.symbols.identifiers)

  def executionPlan() = "HashJoin"
}
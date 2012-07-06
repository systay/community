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

import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.symbols.AnyType
import org.neo4j.cypher.internal.commands.ReturnItem
import org.neo4j.cypher.internal.symbols.Identifier

//This class will extract properties and other stuff to make the maps
//easy to work with for other pipes
class ExtractPipe(source: Pipe, val expressions: Map[String, Expression]) extends PipeWithSource(source) {
  def dependencies = {
    val seq: Seq[Identifier] = expressions.values.flatMap(_.dependencies(AnyType())).toSeq
    println(seq)
    seq
  }

  def getSymbolType(item: ReturnItem): Identifier = item.identifier

  val symbols: SymbolTable = source.symbols.add(expressions.values.toSeq.map(_.identifier):_*)

  val symbols2: SymbolTable2 = source.symbols2.add(expressions.map {
    case (name, expression) => name -> expression.evaluateType(AnyType(), source.symbols2)
  })

  def createResults(state: QueryState) = {
    source.createResults(state).map(subgraph => {
      expressions.foreach {
        case (name, expression) => subgraph += name -> expression(subgraph)
      }

      subgraph
    })
  }

  override def executionPlan(): String = source.executionPlan() + "\r\nExtract([" + source.symbols.keys.mkString(",") + "] => [" + expressions.keys.mkString(", ") + "])"

  def deps = {
    val map  = expressions.map(_._2.identifierDependencies(AnyType())).toSeq
    mergeDeps(map)
  }
}


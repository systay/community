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

import org.neo4j.cypher.internal.commands.ReturnItem
import org.neo4j.cypher.internal.symbols.{SymbolTable2, AnyType, Identifier, SymbolTable}
import org.neo4j.cypher.internal.commands.expressions.{Entity, CachedExpression, ParameterValue}

class ColumnFilterPipe(source: Pipe, val returnItems: Seq[ReturnItem], lastPipe: Boolean)
  extends PipeWithSource(source) {
  val returnItemNames = returnItems.map(_.columnName)
  val symbols = new SymbolTable(identifiers: _*)
  val symbols2 = new SymbolTable2(identifiers2.toMap)

  private lazy val identifiers2: Seq[(String, AnyType)] = returnItems.
    map( ri => ri.name->ri.expression.evaluateType(AnyType(), source.symbols2))

  private lazy val identifiers = source.symbols.identifiers.flatMap {
    // Yay! My first monad!
    case id => returnItems.
      find(ri => ri.expression.identifier.name == id.name).
      map(x => Identifier(x.columnName, id.typ))
  }

  def createResults(state: QueryState) = {
    source.createResults(state).map(ctx => {
      val newMap = MutableMaps.create(ctx.size)

      returnItems.foreach {
        case ReturnItem(Entity(oldName), newName, _)              => newMap.put(newName, ctx(oldName))
        case ReturnItem(CachedExpression(oldName, _), newName, _) => newMap.put(newName, ctx(oldName))
        case ReturnItem(_, name, _)                               => newMap.put(name, ctx(name))
      }

      if (!lastPipe) {
        ctx.foreach {
          case (k, p: ParameterValue) => newMap.put(k, p)
          case _ =>
        }
      }

      ctx.newFrom( newMap )
    })
  }

  override def executionPlan(): String =
    "%s\r\nColumnFilter([%s] => [%s])".format(source.executionPlan(), source.symbols.keys, returnItemNames.mkString(","))

  def dependencies = Seq()

  def deps = mergeDeps(returnItems.map(_.deps))
}
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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.symbols.{SymbolTable, CypherType, ScalarType}
import collection.Map
import org.neo4j.cypher.internal.pipes.ExecutionContext

case class Null() extends Expression {
  def apply(v1: ExecutionContext) = null

  def rewrite(f: (Expression) => Expression): Expression = f(this)

  def filter(f: (Expression) => Boolean): Seq[Expression] = if (f(this)) Seq(this) else Seq()

  def calculateType(symbols: SymbolTable): CypherType = ScalarType()

  def symbolTableDependencies = Set()
}
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

import org.neo4j.cypher.internal.symbols.{SymbolTable2, CypherType, ScalarType, Identifier}
import collection.Map

case class Null() extends Expression {
  protected def compute(v1: Map[String, Any]) = null

  val identifier: Identifier = Identifier("null", ScalarType())

  def declareDependencies(extectedType: CypherType): Seq[Identifier] = Seq()

  def rewrite(f: (Expression) => Expression): Expression = f(this)

  def filter(f: (Expression) => Boolean): Seq[Expression] = if (f(this)) Seq(this) else Seq()

  def identifierDependencies(expectedType: CypherType) = Map()

  def calculateType(symbols: SymbolTable2): CypherType = ScalarType()

  def symbolTableDependencies = Set()
}
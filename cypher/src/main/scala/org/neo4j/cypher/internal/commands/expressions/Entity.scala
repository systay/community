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

import org.neo4j.graphdb.NotFoundException
import org.neo4j.cypher.internal.symbols._
import collection.Map
import org.neo4j.cypher.internal.symbols.Identifier
import org.neo4j.helpers.ThisShouldNotHappenError

case class Entity(entityName: String) extends CastableExpression with Typed {
  def compute(m: Map[String, Any]): Any = m.getOrElse(entityName, throw new NotFoundException("Failed to find `" + entityName + "`"))

  val identifier: Identifier = Identifier(entityName, AnyType())

  override def toString(): String = entityName

  def declareDependencies(extectedType: CypherType): Seq[Identifier] = Seq(Identifier(entityName, extectedType))

  def rewrite(f: (Expression) => Expression) = f(this)

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this)
  else
    Seq()

  def identifierDependencies(expectedType: CypherType) = Map(entityName -> expectedType)

  def calculateType(symbols: SymbolTable2) =
    throw new ThisShouldNotHappenError("Andres", "This class should override evaluateType, and this method should never be run")

  override def evaluateType[T <: CypherType](expectedType: T, symbols: SymbolTable2) = symbols.evaluateType(entityName, expectedType)
}
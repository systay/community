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

import org.neo4j.cypher.internal.symbols.{SymbolTable2, CypherType, AnyType, Identifier}
import org.neo4j.cypher.ParameterNotFoundException
import collection.Map

case class ParameterExpression(parameterName: String) extends CastableExpression {
  def compute(m: Map[String, Any]): Any = {
    val getFromMap: Any = m.getOrElse("-=PARAMETER=-" + parameterName + "-=PARAMETER=-", throw new ParameterNotFoundException("Expected a parameter named " + parameterName))

    getFromMap match {
      case ParameterValue(x) => x
      case _                 => throw new ParameterNotFoundException("Expected a parameter named " + parameterName)
    }
  }

  override def apply(m: Map[String, Any]) = compute(m)

  val identifier: Identifier = Identifier(parameterName, AnyType())

  override def toString(): String = "{" + parameterName + "}"

  def declareDependencies(extectedType: CypherType): Seq[Identifier] = Seq()

  def rewrite(f: (Expression) => Expression) = f(this)

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this)
  else
    Seq()

  def identifierDependencies(expectedType: CypherType) = Map()

  def getType(symbols: SymbolTable2) = AnyType()
}

case class ParameterValue(value: Any)
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

import org.neo4j.cypher.internal.symbols.{AnyType, Identifier, CypherType}
import collection.Map

case class CoalesceFunction(expressions: Expression*) extends Expression {
  def compute(m: Map[String, Any]): Any = expressions.toStream.map(expression => expression(m)).find(value => value != null) match {
    case None    => null
    case Some(x) => x
  }

  def innerExpectedType: Option[CypherType] = None

  val argumentsString: String = expressions.map(_.identifier.name).mkString(",")

  //TODO: Find out the closest matching return type
  val identifier = Identifier("COALESCE(" + argumentsString + ")", AnyType())

  override def toString() = "coalesce(" + argumentsString + ")"

  def declareDependencies(extectedType: CypherType): Seq[Identifier] = expressions.flatMap(_.dependencies(AnyType()))

  def rewrite(f: (Expression) => Expression) = f(CoalesceFunction(expressions.map(e => e.rewrite(f)): _*))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ expressions.flatMap(_.filter(f))
  else
    expressions.flatMap(_.filter(f))

  def identifierDependencies(expectedType: CypherType) = mergeDeps(expressions.map(_.identifierDependencies(AnyType())))
}
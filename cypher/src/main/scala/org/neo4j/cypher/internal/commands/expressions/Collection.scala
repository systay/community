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

import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.symbols.Identifier
import collection.Map

case class Collection(expressions: Expression*) extends CastableExpression {
  def compute(m: Map[String, Any]): Any = expressions.map(e => e(m))

  val identifier: Identifier = Identifier(name, AnyIterableType())

  private def name = expressions.map(_.identifier.name).mkString("[", ", ", "]")

  def declareDependencies(extectedType: CypherType): Seq[Identifier] = expressions.flatMap(_.declareDependencies(AnyType()))

  def rewrite(f: (Expression) => Expression): Expression = f(Collection(expressions.map(f): _*))

  def filter(f: (Expression) => Boolean): Seq[Expression] = if (f(this))
    Seq(this) ++ expressions.flatMap(_.filter(f))
  else
    expressions.flatMap(_.filter(f))

  def identifierDependencies(expectedType: CypherType) = mergeDeps(expressions.map(_.identifierDependencies(ScalarType())))

  def calculateType(symbols: SymbolTable2): CypherType = {
    expressions.map(_.evaluateType(AnyType(), symbols)) match {

      case Seq() => AnyIterableType()

      case types =>
        val innerType = types.foldLeft(AnyType().asInstanceOf[CypherType])(_ mergeWith _)
        new IterableType(  innerType )
    }

  }

  def symbolTableDependencies = expressions.flatMap(_.symbolTableDependencies).toSet
}
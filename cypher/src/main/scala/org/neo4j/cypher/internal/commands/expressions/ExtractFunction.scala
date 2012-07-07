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

import org.neo4j.cypher.internal.commands.IterableSupport
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.symbols.Identifier
import collection.Map

case class ExtractFunction(iterable: Expression, id: String, expression: Expression)
  extends NullInNullOutExpression(iterable)
  with IterableSupport {
  def compute(value: Any, m: Map[String, Any]) = makeTraversable(value).map(iterValue => {
    val innerMap = m + (id -> iterValue)
    expression(innerMap)
  }).toList

  val identifier = Identifier("extract(" + id + " in " + iterable.identifier.name + " : " + expression.identifier.name + ")", new IterableType(expression.identifier.typ))

  def declareDependencies(extectedType: CypherType): Seq[Identifier] =
  // Extract depends on everything that the iterable and the expression depends on, except
  // the new identifier inserted into the expression context, named with id
    iterable.dependencies(AnyIterableType()) ++ expression.dependencies(AnyType()).filterNot(_.name == id)

  def rewrite(f: (Expression) => Expression) = f(ExtractFunction(iterable.rewrite(f), id, expression.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ iterable.filter(f) ++ expression.filter(f)
  else
    iterable.filter(f) ++ expression.filter(f)

  def identifierDependencies(expectedType: CypherType) = {
    val mergedDeps: Map[String, CypherType] = mergeDeps(iterable.identifierDependencies(AnyIterableType()), expression.identifierDependencies(AnyType()))

    // Extract depends on everything that the iterable and the expression depends on, except
    // the new identifier inserted into the expression context, named with id
    mergedDeps.filterKeys(_ != id)
  }

  def calculateType(symbols: SymbolTable2): CypherType = {
    val typeOfElementsInCollection = iterable.evaluateType(AnyIterableType(), symbols).iteratedType

    expression.evaluateType(AnyType(), symbols.add(id, typeOfElementsInCollection))
  }
}
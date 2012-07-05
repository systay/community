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

import org.neo4j.cypher.internal.commands.{IterableSupport, Predicate}
import org.neo4j.cypher.internal.symbols._
import collection.Map
import org.neo4j.cypher.internal.symbols.Identifier

case class FilterFunction(collection: Expression, id: String, predicate: Predicate)
  extends NullInNullOutExpression(collection)
  with IterableSupport {
  def compute(value: Any, m: Map[String, Any]) = makeTraversable(value).filter(element => predicate.isMatch(m + (id -> element)))

  val identifier = Identifier("filter(%s in %s : %s)".format(id, collection.identifier.name, predicate), collection.identifier.typ)

  def declareDependencies(extectedType: CypherType): Seq[Identifier] = (collection.dependencies(AnyIterableType()) ++ predicate.dependencies).filterNot(_.name == id)

  def rewrite(f: (Expression) => Expression) = f(FilterFunction(collection.rewrite(f), id, predicate.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ collection.filter(f)
  else
    collection.filter(f)

  def identifierDependencies(expectedType: CypherType) = {
    val mergedDeps: Map[String, CypherType] = mergeDeps(collection.identifierDependencies(AnyIterableType()), predicate.identifierDependencies(AnyType()))

    // Filter depends on everything that the iterable and the predicate depends on, except
    // the new identifier inserted into the predicate symbol table, named with id
    mergedDeps.filterKeys(_ != id)
  }

  def getType(symbols: SymbolTable2): CypherType = {
    val t = collection.evaluateType(AnyIterableType(), symbols)

    predicate.checkTypes(symbols.add(id, t.iteratedType))

    t
  }
}
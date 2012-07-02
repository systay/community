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

import org.neo4j.graphdb.{Relationship, Node}
import org.neo4j.cypher.internal.symbols.{MapType, CypherType, LongType, Identifier}
import collection.Map

case class IdFunction(inner: Expression) extends NullInNullOutExpression(inner) {

  def compute(value: Any, m: Map[String, Any]) = value match {
    case node: Node        => node.getId
    case rel: Relationship => rel.getId
  }

  val identifier = Identifier("ID(" + inner.identifier.name + ")", LongType())

  def declareDependencies(extectedType: CypherType): Seq[Identifier] = inner.dependencies(MapType())

  def rewrite(f: (Expression) => Expression) = f(IdFunction(inner.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ inner.filter(f)
  else
    inner.filter(f)

  def identifierDependencies(expectedType: CypherType) = inner.identifierDependencies(MapType())
}
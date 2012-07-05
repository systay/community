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

import org.neo4j.graphdb.{NotFoundException, PropertyContainer}
import org.neo4j.cypher.EntityNotFoundException
import org.neo4j.cypher.internal.symbols._
import collection.Map
import org.neo4j.cypher.internal.symbols.Identifier

case class Property(entity: String, property: String) extends CastableExpression {
  def compute(m: Map[String, Any]): Any = {
    m(entity).asInstanceOf[PropertyContainer] match {
      case null              => null
      case propertyContainer => try {
        propertyContainer.getProperty(property)
      } catch {
        case x: NotFoundException => throw new EntityNotFoundException("The property '%s' does not exist on %s".format(property, propertyContainer), x)
      }
    }
  }

  val identifier: Identifier = Identifier(entity + "." + property, ScalarType())

  def declareDependencies(extectedType: CypherType): Seq[Identifier] = Seq(Identifier(entity, MapType()))

  def rewrite(f: (Expression) => Expression) = f(this)

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this)
  else
    Seq()

  def identifierDependencies(expectedType: CypherType): Map[String, CypherType] = Map(entity -> MapType())

  def calculateType(symbols: SymbolTable2) = AnyType()
}
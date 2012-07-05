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
import org.neo4j.cypher.internal.commands.IsIterable
import org.neo4j.cypher.CypherTypeException
import collection.Map
import org.neo4j.cypher.internal.symbols.AnyType
import org.neo4j.cypher.internal.symbols.Identifier

case class Add(a: Expression, b: Expression) extends Expression {
  val identifier = Identifier(a.identifier.name + " + " + b.identifier.name, ScalarType())

  def compute(m: Map[String, Any]) = {
    val aVal = a(m)
    val bVal = b(m)

    (aVal, bVal) match {
      case (x: Number, y: Number)         => x.doubleValue() + y.doubleValue()
      case (x: String, y: String)         => x + y
      case (IsIterable(x), IsIterable(y)) => x ++ y
      case (IsIterable(x), y)             => x ++ Seq(y)
      case (x, IsIterable(y))             => Seq(x) ++ y
      case _                              => throw new CypherTypeException("Don't know how to add `" + aVal.toString + "` and `" + bVal.toString + "`")
    }
  }

  def declareDependencies(extectedType: CypherType) = a.declareDependencies(extectedType) ++ b.declareDependencies(extectedType)

  def rewrite(f: (Expression) => Expression) = f(Add(a.rewrite(f), b.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ a.filter(f) ++ b.filter(f)
  else
    a.filter(f) ++ b.filter(f)

  def identifierDependencies(expectedType: CypherType): Map[String, CypherType] = mergeDeps(a.identifierDependencies(AnyType()), b.identifierDependencies(AnyType()))

  def getType(symbols: SymbolTable2): CypherType = {
    val aT = a.evaluateType(AnyType(), symbols)
    val bT = a.evaluateType(AnyType(), symbols)

    (aT.isIterable, bT.isIterable) match {
      case (true,false) => mergeWithCollection(collection = aT, singleElement = bT)
      case (false,true) => mergeWithCollection(collection = bT, singleElement = aT)
      case _ => aT.mergeWith(bT)
    }
  }

  private def mergeWithCollection(collection: AnyType, singleElement: AnyType):CypherType= {
    val iterableType = collection.asInstanceOf[IterableType]
    val mergedInnerType = iterableType.iteratedType.mergeWith(singleElement)
    new IterableType(mergedInnerType)
  }
}
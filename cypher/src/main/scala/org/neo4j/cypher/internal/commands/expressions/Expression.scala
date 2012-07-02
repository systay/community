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

import org.neo4j.cypher._
import internal.pipes.IdentifierDependant
import internal.symbols._
import collection.Map

abstract class Expression extends (Map[String, Any] => Any) with IdentifierDependant {
  protected def compute(v1: Map[String, Any]) : Any
  def apply(m: Map[String, Any]) = m.getOrElse(identifier.name, compute(m))

  val identifier: Identifier
  def declareDependencies(expectedType: CypherType): Seq[Identifier]
  def dependencies(expectedType: CypherType): Seq[Identifier] = {
    val myType = identifier.typ
    if (!expectedType.isAssignableFrom(myType))
      throw new SyntaxException(identifier.name + " expected to be of type " + expectedType + " but it is of type " + identifier.typ)
    declareDependencies(expectedType)
  }

  def rewrite(f: Expression => Expression): Expression
  def exists(f: Expression => Boolean) = filter(f).nonEmpty
  def filter(f: Expression => Boolean): Seq[Expression]
  def subExpressions = filter( _ != this)
  def containsAggregate = exists(_.isInstanceOf[AggregationExpression])

  override def toString() = identifier.name
}


trait Typed {
  def getType:CypherType

  //Will return the closest class that encompasses both types
  def mergeTypes(a: CypherType, b: CypherType): CypherType = {
    if (a.isAssignableFrom(b)) a
    else if (b.isAssignableFrom(a)) b
    else mergeTypes(a.parentType, b.parentType)
  }
}

case class CachedExpression(key:String, identifier:Identifier) extends CastableExpression {
  override def apply(m: Map[String, Any]) = m(key)
  protected def compute(v1: Map[String, Any]) = null
  def declareDependencies(extectedType: CypherType) = Seq()
  def rewrite(f: (Expression) => Expression) = f(this)
  def filter(f: (Expression) => Boolean) = if(f(this)) Seq(this) else Seq()
  override def toString() = "Cached(" + super.toString() + ")"

  def getType = identifier.typ

  def identifierDependencies(expectedType: CypherType) = Map()
}

abstract class Arithmetics(left: Expression, right: Expression) extends Expression {
  val identifier = Identifier("%s %s %s".format(left.identifier.name, operand, right.identifier.name), NumberType())
  def operand: String
  def throwTypeError(bVal: Any, aVal: Any): Nothing = {
    throw new CypherTypeException("Don't know how to " + verb + " `" + name(bVal) + "` with `" + name(aVal) + "`")
  }

  private def name(x:Any)=x match {
    case null => "null"
    case _ => x.toString
  }

  def compute(m: Map[String, Any]) = {
    val aVal = left(m)
    val bVal = right(m)

    (aVal, bVal) match {
      case (x: Number, y: Number) => numberWithNumber(x, y)
      case (x: String, y: String) => stringWithString(x, y)
      case _ => throwTypeError(bVal, aVal)
    }

  }

  def verb: String
  def stringWithString(a: String, b: String): String
  def numberWithNumber(a: Number, b: Number): Number
  def declareDependencies(extectedType: CypherType) = left.declareDependencies(extectedType) ++ right.declareDependencies(extectedType)
  def filter(f: (Expression) => Boolean) = if(f(this))
    Seq(this) ++ left.filter(f) ++ right.filter(f)
  else
    left.filter(f) ++ right.filter(f)

  def identifierDependencies(expectedType: CypherType): Map[String, CypherType] = mergeDeps(left.identifierDependencies(AnyType()), right.identifierDependencies(AnyType()))
}

abstract class CastableExpression extends Expression {
  override def dependencies(extectedType: CypherType): Seq[Identifier] = declareDependencies(extectedType)
}
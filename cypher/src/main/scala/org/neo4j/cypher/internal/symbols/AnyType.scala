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
package org.neo4j.cypher.internal.symbols

import java.lang.String
import org.neo4j.cypher.CypherTypeException

object CypherType {

  def fromJava(obj:Any):CypherType = {
    if(obj.isInstanceOf[String] || obj.isInstanceOf[Char])
      return StringType()

    if(obj.isInstanceOf[Number])
      return NumberType()

    if(obj.isInstanceOf[Boolean])
      return BooleanType()

    if(obj.isInstanceOf[Seq[_]] || obj.isInstanceOf[Array[_]])
      return AnyIterableType()

    AnyType()
  }
}

trait CypherType {
  def isAssignableFrom(other: CypherType): Boolean = this.getClass.isAssignableFrom(other.getClass)

  def iteratedType: CypherType = throw new RuntimeException("wut")

  def mergeWith(other: CypherType): CypherType = {
    if (this.isAssignableFrom(other)) other
    else if (other.isAssignableFrom(this)) this
    else throw new CypherTypeException("Failed merging " + this + " with " + other)
  }

  def parentType : CypherType

  val isIterable: Boolean = false
}

case class AnyType() extends CypherType {
  override def equals(other: Any) = if (other == null)
    false
  else
    other match {
      case x: AnyRef => x.getClass == this.getClass
      case _ => false
    }

  override val iteratedType: CypherType = this

  override def toString: String = this.getClass.getSimpleName

  def parentType:CypherType = this //This is the root of all
}























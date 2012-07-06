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
package org.neo4j.cypher.internal.pipes

import collection.Seq
import collection.Map
import org.neo4j.cypher.internal.symbols.{CypherType, Identifier}

abstract class PipeWithSource(val source: Pipe) extends Pipe with Dependant with IdentifierDependantHelper {
  dependencies.foreach(source.symbols.assertHas(_))

  def deps:Map[String, CypherType]
}
/*
Classes that have dependencies on identifiers and their types

PipeWithSource is then responsible for taking the dependencies of all expressions used by a pipe, and check if the
underlying pipe meets them.
 */
trait IdentifierDependant extends IdentifierDependantHelper {
  /*This is a declaration of the identifiers that this particular expression expects to
  find in the symboltable to be able to run successfully.*/
  def identifierDependencies(expectedType:CypherType):Map[String, CypherType]
}

trait IdentifierDependantHelper {
  def mergeDeps(deps: Seq[Map[String, CypherType]]): Map[String, CypherType] = deps.foldLeft(Map[String, CypherType]()) {
    case (result, current) => mergeDeps(result, current)
  }

  def mergeDeps(a: Map[String, CypherType], b: Map[String, CypherType]): Map[String, CypherType] = {
    val keys = (a.keys ++ b.keys).toSeq.distinct
    val allDeps: Seq[(String, List[CypherType])] = keys.map(key => key -> (a.get(key) ++ b.get(key)).toList.distinct)
    val map: Seq[(String, CypherType)] = allDeps.map {
      case (key, types) => val t: CypherType = types match {
        case List(single: CypherType)               => single
        case List(one: CypherType, two: CypherType) => one.mergeWith(two)
      }
      key -> t
    }
    map.toMap
  }
}



trait Dependant {
  def dependencies: Seq[Identifier]
}
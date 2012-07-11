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
package org.neo4j.cypher.internal.mutation

import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.commands.IterableSupport
import org.neo4j.cypher.internal.symbols.{SymbolTable2, AnyIterableType}
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}

case class ForeachAction(collection: Expression, id: String, actions: Seq[UpdateAction])
  extends UpdateAction
  with IterableSupport {
  def dependencies = {
    val ownIdentifiers = actions.flatMap(_.identifier)

    val updateDeps = actions.flatMap(_.dependencies).
      filterNot(_.name == id). //remove dependencies to the symbol we're introducing
      filterNot(ownIdentifiers contains) //remove dependencies to identifiers we are introducing

    collection.dependencies(AnyIterableType()) ++ updateDeps
  }

  def exec(context: ExecutionContext, state: QueryState) = {
    val before = context.get(id)

    val seq = makeTraversable(collection(context))
    seq.foreach(element => {
      context.put(id, element)

      // We do a fold left here to allow updates to introduce
      // symbols in each others context.
      actions.foldLeft(Seq(context))((contexts, action) => {
        contexts.flatMap(c => action.exec(c, state))
      })
    })

    before match {
      case None => context.remove(id)
      case Some(old) => context.put(id, old)
    }

    Stream(context)
  }

  def filter(f: (Expression) => Boolean) = Some(collection).filter(f).toSeq ++ actions.flatMap(_.filter(f))

  def rewrite(f: (Expression) => Expression) = ForeachAction(f(collection), id, actions.map(_.rewrite(f)))

  def identifier = Seq.empty
  def identifier2 = Seq.empty

  def deps = {

    val actionDeps = mergeDeps(actions.map(_.deps))
    val mergedDeps = mergeDeps(collection.identifierDependencies(AnyIterableType()), actionDeps)

    // ForEach depends on everything that the iterable and the inner update actions depends on,
    // except the identifier inserted into the foreach symbol table
    mergedDeps.filterKeys(_ != id)
  }

  def assertTypes(symbols: SymbolTable2) {
    val t = collection.evaluateType(AnyIterableType(), symbols).iteratedType

    val innerSymbols: SymbolTable2 = symbols.add(id, t)

    actions.foreach(_.assertTypes(innerSymbols))
  }

  def symbolTableDependencies = {
    val updateActionsDeps = actions.flatMap(_.symbolTableDependencies).filterNot(_ == id).toSet
    val collectionDeps = collection.symbolTableDependencies
    updateActionsDeps ++ collectionDeps
  }
}
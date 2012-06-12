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

import org.neo4j.cypher.internal.symbols.Identifier
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.commands.{StartItem, Expression}
import org.neo4j.graphdb.Lock
import org.neo4j.cypher.{PatternException, RelatePathNotUnique}

case class RelateAction(links: RelateLink*) extends UpdateAction {
  def dependencies: Seq[Identifier] = links.flatMap(_.dependencies)

  def exec(context: ExecutionContext, state: QueryState): Traversable[ExecutionContext] = {
    var linksToDo: Seq[RelateLink] = links
    var ctx = context

    while (linksToDo.nonEmpty) {

      val results = linksToDo.map(link => link.exec(ctx, state))

      val result = results.reduce(_ reduceWith _)

      linksToDo = result.leftToDo

      result match {
        case Done()                      => //We're done! Let's go home
        case Traverse(a, _)              => ctx = ctx.newWith(a.toMap)
        case Update(commands, locker, _) => ctx = tryAgainWithLocks(locker, linksToDo, ctx, state)
        case CanNotAdvance(todo)         => throw new PatternException("Unbound relate pattern found " + todo)
      }
    }

    Stream(ctx)
  }


  def tryAgainWithLocks(locker: () => scala.Seq[Lock], linksToDo: scala.Seq[RelateLink], ctx: ExecutionContext, state: QueryState): ExecutionContext = {
    val locks = locker()

    try {
      tryAgain(linksToDo, ctx, state)
    } finally {
      locks.foreach(_.release())
    }
  }

  private def tryAgain(linksToDo: Seq[RelateLink], context: ExecutionContext, state: QueryState): ExecutionContext = {
    val result = linksToDo.
      map(link => link.exec(context, state)).
      reduce(_ reduceWith _)

    result match {
      case Traverse(a, _)              => context.newWith(a.toMap)
      case Update(commands, locker, _) => runUpdateCommands(commands, context, state)
      case _                           => throw new ThisShouldNotHappenError("Andres", "Second relate check should only return traverse or update")
    }
  }

  private def runUpdateCommands(cmds: Seq[UpdateWrapper], startContext: ExecutionContext, state: QueryState): ExecutionContext = {
    var context = startContext
    var todo = cmds.distinct
    var done = Seq[String]()

    while (todo.nonEmpty) {
      val (unfiltered, temp) = todo.partition(_.canRun(context))
      todo = temp

      val current: Seq[UpdateWrapper] = unfiltered.filterNot(cmd => done.contains(cmd.cmd.identifierName))
      done = done ++ current.map(_.cmd.identifierName)

      context = current.foldLeft(context) {
        case (currentContext, updateCommand) =>
          val result = updateCommand.cmd.exec(currentContext, state)
          if (result.size != 1) {
            throw new RelatePathNotUnique("The pattern " + this + " produced multiple possible paths, and that is not allowed")
          } else {
            result.head
          }
      }
    }

    context
  }

  def filter(f: (Expression) => Boolean): Seq[Expression] = links.flatMap(_.filter(f)).distinct

  def identifier: Seq[Identifier] = links.flatMap(_.identifier).distinct

  def rewrite(f: (Expression) => Expression): UpdateAction = RelateAction(links.map(_.rewrite(f)): _*)
}


case class UpdateWrapper(needs: Seq[String], cmd: StartItem with UpdateAction) {
  def canRun(context: ExecutionContext) = {
    val keySet = context.keySet
    needs.forall(keySet.contains)
  }
}


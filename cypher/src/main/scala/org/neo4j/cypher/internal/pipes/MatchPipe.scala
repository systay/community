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

import matching.{PatternGraph, MatchingContext}
import java.lang.String
import org.neo4j.cypher.internal.commands.Predicate

class MatchPipe(source: Pipe, predicates: Seq[Predicate], patternGraph: PatternGraph) extends Pipe {
  val matchingContext = new MatchingContext(source.symbols2, predicates, patternGraph)
  val symbols2 = matchingContext.symbols
//  val symbols = new SymbolTable(symbols2.identifiers.map(x => Identifier(x._1, x._2)).toSeq:_*)

  def createResults(state: QueryState) =
    source.createResults(state).flatMap(ctx => {
      matchingContext.getMatches(ctx.toMap).map(pm => ctx.newWith(pm))
    })

  override def executionPlan(): String = source.executionPlan() + "\r\nPatternMatch(" + patternGraph + ")"
}
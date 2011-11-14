/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.pql

import commands._
import org.junit.Assert._
import org.junit.Test
import parser.PqlParser

class SematicErrorTest extends ExecutionEngineHelper {
  @Test def returnNodeThatsNotThere() {
    expectedError("select bar from x=node(0)",
      """Unknown identifier "bar".""")
  }

  @Test def throwOnDisconnectedPattern() {
    expectedError("select x from x=node(0) pattern a-[rel]->b",
      "All parts of the pattern must either directly or indirectly be connected to at least one bound entity. These identifiers were found to be disconnected: a, b, rel")
  }

  @Test def defineNodeAndTreatItAsARelationship() {
    expectedError("select r from r=node(0) pattern a-[r]->b",
      "Some identifiers are used as both relationships and nodes: r")
  }

  @Test def redefineSymbolInMatch() {
    expectedError("select r from a=node(0) pattern a-[r]->b-->r",
      "Some identifiers are used as both relationships and nodes: r")
  }

  @Test def cantUseTYPEOnNodes() {
<<<<<<< HEAD
    expectedError("select type(r) from r=node(0)",
      "Expected r to be a RelationshipIdentifier but it was NodeIdentifier")
=======
    expectedError("start r=node(0) return type(r)",
      "Expected `r` to be a RelationshipIdentifier but it was NodeIdentifier")
>>>>>>> master
  }

  @Test def cantUseLENGTHOnNodes() {
    expectedError("select length(n) from n=node(0)",
      "Expected n to be an iterable, but it is not.")
  }

  @Test def cantReUseRelationshipIdentifier() {
    expectedError("select r from a=node(0) pattern a-[r]->b-[r]->a",
      "Can't re-use pattern relationship 'r' with different start/end nodes.")
  }

  @Test def shortestPathNeedsBothEndNodes() {
    expectedError("select p from n=node(0) pattern p=shortestPath(n-->b)",
      "Shortest path needs both ends of the path to be provided. Couldn't find b")
  }

  def parse(txt:String):Query = new PqlParser().parse(txt)

  def expectedError(query: String, message: String) { expectedError(parse(query), message) }

  def expectedError(query: Query, message: String) {
    try {
      execute(query).toList
      fail("Did not get the expected syntax error, expected: " + message)
    } catch {
      case x: SyntaxException => assertEquals(message, x.getMessage)
    }
  }
}
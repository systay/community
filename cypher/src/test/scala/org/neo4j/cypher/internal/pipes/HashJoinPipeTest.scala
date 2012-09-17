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

import org.junit.Test
import org.neo4j.cypher.internal.symbols.{StringType, NumberType}
import org.scalatest.Assertions

class HashJoinPipeTest extends Assertions {
  @Test def should_not_return_when_missing_matches() {
    val a = new FakePipe(Seq(Map("a" -> 0)), "a" -> NumberType())
    val b = new FakePipe(Seq(Map("a" -> 1)), "a" -> NumberType())
    val joiner = new HashJoinPipe(a, b)

    assert(joiner.createResults(QueryState()).isEmpty, "We should not find any matches")
  }

  @Test def should_complain_about_missing_overlaps() {
    val a = new FakePipe(Seq(Map("a" -> 0)), "a" -> NumberType())
    val b = new FakePipe(Seq(Map("b" -> 1)), "b" -> NumberType())

    intercept[AssertionError](new HashJoinPipe(a, b))
  }

  @Test def should_join_stuff() {
    val a = new FakePipe(Seq(Map("a" -> 0)), "a" -> NumberType())
    val b = new FakePipe(Seq(Map("a" -> 0)), "a" -> NumberType())

    val joiner = new HashJoinPipe(a, b)

    val results = joiner.createResults(QueryState()).toList

    assert(results === List(Map("a" -> 0)))
  }

  @Test def should_join_stuff2() {
    val a = new FakePipe(Seq(Map("a" -> 0, "b" -> "Andres")), "a" -> NumberType(), "b" -> StringType())
    val b = new FakePipe(Seq(Map("a" -> 0, "c" -> "Stefan")), "a" -> NumberType(), "c" -> StringType())

    val joiner = new HashJoinPipe(a, b)

    val results = joiner.createResults(QueryState()).toList

    assert(results === List(Map("a" -> 0, "b" -> "Andres", "c" -> "Stefan")))
  }

  @Test def should_join_stuff3() {
    val a = new FakePipe(Seq(
      Map("a" -> 0, "b" -> "Andres1"),
      Map("a" -> 1, "b" -> "Andres2")
    ), "a" -> NumberType(), "b" -> StringType())
    val b = new FakePipe(Seq(
      Map("a" -> 0, "c" -> "Stefan1"),
      Map("a" -> 2, "c" -> "Stefan2")
    ), "a" -> NumberType(), "c" -> StringType())

    val joiner = new HashJoinPipe(a, b)

    val results = joiner.createResults(QueryState()).toList

    assert(results === List(Map("a" -> 0, "b" -> "Andres1", "c" -> "Stefan1")))
  }
}
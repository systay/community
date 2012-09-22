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

import nestedHashJoin.NestedHashJoin
import org.scalatest.Assertions
import org.junit.Test

class NestedHashJoinTest extends Assertions {

  @Test def two_empty_inputs_should_return_empty() {
    assert(createJoiner(
      //Inputs
      Seq.empty,
      Seq.empty).toList ===

      //Expectation
      Seq.empty)
  }

  @Test def left_empty_input_should_return_empty() {
    assert(createJoiner(
      //Inputs
      Seq(Map.empty),
      Seq.empty).toList ===

      //Expectation
      Seq.empty)
  }

  @Test def right_empty_input_should_return_empty() {
    assert(createJoiner(
      //Inputs
      Seq(Map.empty),
      Seq.empty).toList ===

      //Expectation
      Seq.empty)
  }

  @Test def two_matching_single_element_maps() {
    assert(createJoiner(
      //Inputs
      Seq(Map("key1" -> "s")),
      Seq(Map("key1" -> "s"))).toList ===

      //Expectation
      Seq(Map("key1" -> "s")))
  }

  @Test def different_sizes_left_is_larger() {
    assert(createJoiner(
      //Inputs
      Seq(Map("key1" -> "s"), Map("key1" -> "s")),
      Seq(Map("key1" -> "s"))).toList ===

      //Expectation
      Seq(Map("key1" -> "s"), Map("key1" -> "s")))
  }

  @Test def different_sizes_right_is_larger() {
    assert(createJoiner(
      //Inputs
      Seq(Map("key1" -> "s")),
      Seq(Map("key1" -> "s"), Map("key1" -> "s"))).toList ===

      //Expectation
      Seq(Map("key1" -> "s"), Map("key1" -> "s")))
  }

  @Test def should_return_empty_if_no_matches_exist() {
    assert(createJoiner(
      Seq(Map("key1" -> "a"), Map("key1" -> "b")),
      Seq(Map("key1" -> "c"), Map("key1" -> "d"))).toList ===

      //Expectation
      Seq())
  }

  @Test def left_input_larger_than_max() {
    assert(createJoiner(
      //Inputs
      Seq(Map("key1" -> "1")),
      Seq(Map("key1" -> "1"), Map("key1" -> "2"), Map("key1" -> "3"), Map("key1" -> "4"), Map("key1" -> "5")),

      //MAX
      max = 3).toSet ===

      //Expectation
      Set(Map("key1" -> "1")))
  }

  @Test def right_input_larger_than_max() {
    assert(createJoiner(
      //Inputs
      Seq(Map("key1" -> "1"), Map("key1" -> "2"), Map("key1" -> "3"), Map("key1" -> "4"), Map("key1" -> "5")),
      Seq(Map("key1" -> "1")),

      //MAX
      max = 3).toSet ===

      //Expectation
      Set(Map("key1" -> "1")))
  }

  @Test def both_inputs_larger_than_max() {
    assert(createJoiner(
      //Inputs
      Seq(Map("key1" -> "1"), Map("key1" -> "2"), Map("key1" -> "3"), Map("key1" -> "4"), Map("key1" -> "5")),
      Seq(Map("key1" -> "4"), Map("key1" -> "2"), Map("key1" -> "6"), Map("key1" -> "5")),

      //MAX
      max = 3).toSeq ===

      //Expectation
      Seq(Map("key1" -> "2"), Map("key1" -> "4"), Map("key1" -> "5")))
  }


  private def createJoiner(a: Traversable[Map[String, Any]], b: Traversable[Map[String, Any]], max: Int = 10) =
    new NestedHashJoin(a, b, map => (1 to 10).flatMap(x => map.get("key" + x)), max)
}
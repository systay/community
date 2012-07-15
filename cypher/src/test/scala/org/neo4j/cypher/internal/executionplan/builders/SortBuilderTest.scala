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
package org.neo4j.cypher.internal.executionplan.builders

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.SortItem
import org.neo4j.cypher.internal.symbols.{AnyType, ScalarType, Identifier}
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.neo4j.cypher.internal.commands.expressions.{CachedExpression, Property}

class SortBuilderTest extends BuilderTest {

  val builder = new SortBuilder

  @Test def should_accept_if_all_work_is_done_and_sorting_not_yet() {
    val q = PartiallySolvedQuery().copy(
      sort = Seq(Unsolved(SortItem(Property("x", "foo"), ascending = true))),
      extracted = true
    )

    val p = createPipe(nodes = Seq("x"))

    assertTrue("Builder should accept this", builder.canWorkWith(plan(p, q)))
  }

  @Test def should_not_accept_if_not_yet_extracted() {
    val q = PartiallySolvedQuery().copy(
      sort = Seq(Unsolved(SortItem(Property("x", "foo"), ascending = true))),
      extracted = false
    )

    val p = createPipe(nodes = Seq("x"))

    assertFalse("Builder should accept this", builder.canWorkWith(plan(p, q)))
  }
}
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
package org.neo4j.cypher.internal.pipes.matching

import org.scalatest.Assertions
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.symbols.{NodeType, Identifier, SymbolTable}
import org.junit.{Ignore, Test}

class PatternGraphTest extends Assertions {

  var nodes = Map[String, PatternNode]()
  var rels = Map[String, PatternRelationship]()

  @Test def double_optional_paths_recognized_as_such() {
    //given a-[?]->x<-[?]-b, where a and b are bound
    val a = createNode("a")
    val x = createNode("x")
    val b = createNode("b")
    val r1 = relate(a, x, "r1")
    val r2 = relate(b, x, "r2")
    val symbols = bind("a", "b")

    //when
    val graph = new PatternGraph(nodes, rels, symbols)

    //then
    assert(graph.doubleOptionalPaths.toSeq === Seq(DoubleOptionalPath(Seq(a,r1,x,r2,b))))
  }


  def createNode(name: String): PatternNode = {
    val node = new PatternNode(name)
    nodes = nodes + (name -> node)
    node
  }

  @Test def should_handle_two_optional_paths_between_two_pattern_nodes() {
    //given a-[r1?]->x<-[r2?]-b,a<-[r3?]-z-[r4?]->b   where a and b are bound
    val a = createNode("a")
    val x = createNode("x")
    val b = createNode("b")
    val z = createNode("z")

    val r1 = relate(a, x, "r1")
    val r2 = relate(b, x, "r2")
    val r3 = relate(z, a, "r3")
    val r4 = relate(z, b, "r4")

    val symbols = bind("a", "b")

    //when
    val graph = new PatternGraph(nodes, rels, symbols)

    //then
    assert(graph.doubleOptionalPaths.toSet === Set(DoubleOptionalPath(Seq(a, r1, x, r2, b)), DoubleOptionalPath(Seq(a, r3, z, r4, b))))
  }

  @Test def should_trim_away() {
    //given a-[r1]->x-[r2?]->()<-[r3?]-z<-[r4]-b   where a and b are bound
    val a = createNode("a")
    val b = createNode("b")
    val unknown = createNode("unknown")
    val z = createNode("z")
    val x = createNode("x")

    val r1 = relate(a, x, "r1", optional = false)
    val r2 = relate(x, unknown, "r2")
    val r3 = relate(z, unknown, "r3")
    val r4 = relate(b, z, "r4", optional = false)

    val symbols = bind("a", "b")

    //when
    val graph = new PatternGraph(nodes, rels, symbols)

    //then we should find the shortest possible DOP
    assert(graph.doubleOptionalPaths.toSet === Set(DoubleOptionalPath(Seq(x,r2,unknown,r3,z))))
  }

  @Ignore("Not strictly neccessary, more of a performance thing")
  @Test def dop_with_connected_middle_not_emitted() {
    //given a-[r1?]->x<-[r2?]-b, c-[r3]->x   where a, b and c are bound
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val x = createNode("x")

    relate(a, x, "r1")
    relate(b, x, "r2")
    relate(c, x, "r3", optional = false)

    val symbols = bind("a", "b", "c")

    //when
    val graph = new PatternGraph(nodes, rels, symbols)

    //then we should find the shortest possible DOP
    assert(graph.doubleOptionalPaths === Seq())
  }


  private def relate(a: PatternNode, x: PatternNode, key: String): PatternRelationship = relate(a, x, key, optional = true)

  private def relate(a: PatternNode, x: PatternNode, key: String, optional: Boolean): PatternRelationship = {
    val r = a.relateTo(key, x, Seq(), Direction.OUTGOING, optional, predicate = True())
    rels = rels + (key -> r)
    r
  }

  private def bind(boundSymbols: String*): SymbolTable = {
    val identifiersToCreate = boundSymbols.map(x => Identifier(x, NodeType()))
    new SymbolTable(identifiersToCreate: _*)
  }

}
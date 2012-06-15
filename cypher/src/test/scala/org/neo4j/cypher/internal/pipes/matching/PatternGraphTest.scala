package org.neo4j.cypher.internal.pipes.matching

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.symbols.{NodeType, Identifier, SymbolTable}

class PatternGraphTest extends Assertions {

  var nodes = Map[String, PatternNode]()
  var rels = Map[String, PatternRelationship]()

  @Test def double_optional_paths_recognized_as_such() {
    //given a-[?]->x<-[?]-b, where a and b are bound
    val a = createNode("a")
    val x = createNode("x")
    val b = createNode("b")
    relate(a, x, "r1")
    relate(b, x, "r2")
    val symbols = bind("a", "b")

    //when
    val graph = new PatternGraph(nodes, rels, symbols)

    //then
    assert(graph.doubleOptionalPaths.toSeq === Seq(DoubleOptionalPath.create("a", "b", "r1", "r2")))
  }


  def createNode(name: String): PatternNode = {
    val node = new PatternNode(name)
    nodes = nodes + (name -> node)
    node
  }

  @Test def should_handle_two_optional_paths_between_two_pattern_nodes() {
    //given a-[r1?]->x<-[r2?]-b,a<-[r3?]-x-[r4?]->b   where a and b are bound
    val a = createNode("a")
    val x = createNode("x")
    val b = createNode("b")
    val z = createNode("z")

    relate(a, x, "r1")
    relate(b, x, "r2")
    relate(z, a, "r3")
    relate(z, b, "r4")

    val symbols = bind("a", "b")

    //when
    val graph = new PatternGraph(nodes, rels, symbols)

    //then
    assert(graph.doubleOptionalPaths.toSet === Set(DoubleOptionalPath("a", "b", Seq("r1", "r3"), Seq("r2", "r4"))))
  }


  private def relate(a: PatternNode, x: PatternNode, key: String): PatternRelationship = {
    val r = a.relateTo(key, x, Seq(), Direction.OUTGOING, optional = true, predicate = True())
    rels = rels + (key -> r)
    r
  }

  private def bind(boundSymbols: String*): SymbolTable = {
    val identifiersToCreate = boundSymbols.map(x => Identifier(x, NodeType()))
    new SymbolTable(identifiersToCreate: _*)
  }

}
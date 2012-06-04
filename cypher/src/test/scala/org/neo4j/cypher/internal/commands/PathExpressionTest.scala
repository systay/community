package org.neo4j.cypher.internal.commands

import org.neo4j.cypher.GraphDatabaseTestBase
import org.scalatest.Assertions
import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.{Path, Direction}

class PathExpressionTest extends GraphDatabaseTestBase with Assertions {
  @Test def shouldAcceptShortestPathExpressions() {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val pattern = ShortestPath(
      pathName = "p",
      start = "a",
      end = "c",
      relTypes = Seq(),
      dir = Direction.OUTGOING,
      maxDepth = None,
      optional = false,
      single = true,
      relIterator = None,
      predicate = True())

    val expression = ShortestPathExpression(pattern)

    val m = Map("a" -> a, "c" -> c)

    val result = expression(m).asInstanceOf[Seq[Path]].head

    assertEquals(result.startNode(), a)
    assertEquals(result.endNode(), c)
    assertEquals(result.length(), 2)
  }
}
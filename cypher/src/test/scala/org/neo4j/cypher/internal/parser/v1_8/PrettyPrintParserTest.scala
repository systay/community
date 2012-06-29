package org.neo4j.cypher.internal.parser.v1_8

import org.junit.Test
import org.scalatest.Assertions


class PrettyPrintParserTest extends Assertions {
  val parser = new PrettyPrintParser

  @Test def simpleStart() {
    assert(parser.parse("start n=node(1) return n") === "START n=node(1)\nRETURN n")
  }
}

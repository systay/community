package org.neo4j.cypher.internal.parser.v1_8

import util.parsing.combinator.RegexParsers


/*
2012-06-29 Andres Taylor: This should really be done using the normal parser and the query objects. This is a temporary workaround
until we can re-structure the existing parser to do this.
*/
class PrettyPrintParser extends RegexParsers {
  def parse(text: String): String = text
}
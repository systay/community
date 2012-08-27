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
package org.neo4j.cypher.internal.parser.v1_9

import util.parsing.combinator.JavaTokenParsers
import org.neo4j.helpers.ThisShouldNotHappenError

trait Keywords extends JavaTokenParsers {
   private def internal_keywords = List("create", "start", "node", "relationship", "rel", "and", "or", "unique", "match",
     "descending", "desc", "ascending", "asc", "order", "by", "foreach", "in", "delete", "set", "skip", "limit", "true",
     "false", "null", "where", "distinct", "is", "not", "return", "as", "with")

  def CREATE = keyword(0)
  def START = keyword(1)
  def NODE = keyword(2)
  def REL = (keyword(3) | keyword(4)) ^^^ "rel"
  def AND = keyword(5)
  def OR = keyword(6)
  def UNIQUE = keyword(7)
  def MATCH = keyword(8)
  def DESC = keyword(9) | keyword(10)
  def ASC = keyword(11) | keyword(12)
  def ORDER = keyword(13)
  def BY = keyword(14)
  def FOREACH = keyword(15)
  def IN = keyword(16)
  def DELETE = keyword(17)
  def SET = keyword(18)
  def SKIP = keyword(19)
  def LIMIT = keyword(20)
  def TRUE = keyword(21)
  def FALSE = keyword(22)
  def NULL = keyword(23)
  def WHERE = keyword(24)
  def DISTINCT = keyword(25)
  def IS = keyword(26)
  def NOT = keyword(27)
  def RETURN = keyword(28)
  def AS = keyword(29)
  def WITH = keyword(30)

  private def functions = List("shortestpath", "allshortestpaths", "extract", "coalesce", "filter", "count", "all",
    "any", "none", "single", "sum", "min", "max", "avg", "collect", "has")

  def SHORTESTPATH = function(0)
  def ALLSHORTESTPATHS = function(1)
  def EXTRACT = function(2)
  def COALESCE= function(3)
  def FILTER = function(4)
  def COUNT= function(5)
  def ALL = function(6)
  def ANY = function(7)
  def NONE = function(8)
  def SINGLE = function(9)
  def SUM = function(10)
  def MIN = function(11)
  def MAX = function(12)
  def AVG = function(13)
  def COLLECT = function(14)
  def HAS = function(15)


  private def keyword(idx: Int): Parser[String] = ignoreCase(internal_keywords(idx))
  private def function(idx: Int): Parser[String] = ignoreCase(functions(idx))

  def keywords:Parser[String] = ignoreCases(internal_keywords)

  private def ignoreCases(strings: List[String]): Parser[String] = strings match {
    case List(x)       => ignoreCase(x)
    case first :: rest => ignoreCase(first) | ignoreCases(rest)
    case _             => throw new ThisShouldNotHappenError("Andres", "Something went wrong if we get here.")
  }

  private def ignoreCase(str: String): Parser[String] = ("""(?i)\b""" + str + """\b""").r ^^ (x => x.toLowerCase)

}
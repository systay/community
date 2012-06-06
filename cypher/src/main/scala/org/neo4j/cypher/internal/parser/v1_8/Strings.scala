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
package org.neo4j.cypher.internal.parser.v1_8

trait Strings extends Base {
  //KEYWORDS
  def WHERE = ignoreCase("where")
  def FOREACH = ignoreCase("foreach")
  def IN = ignoreCase("in")
  def DELETE = ignoreCase("delete")
  def SET = ignoreCase("set")
  def RELATE = ignoreCase("relate")
  def START = ignoreCase("start")
  def NODE = ignoreCase("node")
  def RELATIONSHIP = ignoreCase("relationship")
  def REL = ignoreCase("rel")
  def CREATE = ignoreCase("create")
  def RETURN = ignoreCase("return")
  def AS = ignoreCase("as")
  def DISTINCT = ignoreCase("distinct")
  def WITH = ignoreCase("with")
  def SKIP = ignoreCase("skip")
  def LIMIT = ignoreCase("limit")
  def AND = ignoreCase("and")
  def OR = ignoreCase("or")
  def NOT = ignoreCase("not")
  def IS = ignoreCase("IS")
  def ORDER = ignoreCase("order")
  def BY = ignoreCase("by")
  def DESCENDING = ignoreCases("descending", "desc")
  def ASCENDING = ignoreCases("ascending", "asc")
  def MATCH = ignoreCase("match")
  def TRUE = ignoreCase("true")
  def FALSE = ignoreCase("false")
  def NULL = ignoreCase("null")

  //FUNCTIONS
  def HAS = ignoreCase("has")
  def ALL = ignoreCase("all")
  def ANY = ignoreCase("any")
  def NONE = ignoreCase("none")
  def SINGLE = ignoreCase("single")
  def SHORTEST_PATH = ignoreCase("shortestpath")
  def ALL_SHORTEST_PATHS = ignoreCase("allShortestPaths")
  def EXTRACT = ignoreCase("extract")
  def COALESCE = ignoreCase("coalesce")
  def FILTER = ignoreCase("filter")

  def TYPE = ignoreCase("type")
  def ID = ignoreCase("id")
  def LENGTH = ignoreCase("length")
  def NODES = ignoreCase("nodes")
  def RELS = ignoreCase("rels")
  def RELATIONSHIPS = ignoreCase("relationships")
  def ABS = ignoreCase("abs")
  def ROUND = ignoreCase("round")
  def SQRT = ignoreCase("sqrt")
  def SIGN = ignoreCase("sign")
  def HEAD = ignoreCase("head")
  def LAST = ignoreCase("last")
  def TAIL = ignoreCase("tail")
  def COUNT = ignoreCase("count")
  def SUM = ignoreCase("sum")
  def MIN = ignoreCase("min")
  def MAX = ignoreCase("max")
  def AVG = ignoreCase("avg")
  def COLLECT = ignoreCase("collect")
}

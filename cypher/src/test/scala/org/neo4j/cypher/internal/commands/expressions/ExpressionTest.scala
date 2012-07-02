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
package org.neo4j.cypher.internal.commands.expressions

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.symbols._
import collection.Map
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.symbols.Identifier

class ExpressionTest extends Assertions {
  @Test def replacePropWithCache() {
    val a = Collect(Nullable(Property("r", "age")))

    val b = a.rewrite {
      case Property(n, p) => Literal(n + "." + p)
      case x              => x
    }

    assert(b === Collect(Nullable(Literal("r.age"))))
  }

  @Test def merge_two_different_identifiers() {
    testMerge(
      Map("a" -> AnyType()),
      Map("b" -> AnyType()),

      Map("a" -> AnyType(), "b" -> AnyType()))
  }

  @Test def merge_two_deps_on_the_same_identifier() {
    testMerge(
      Map("a" -> AnyType()),
      Map("a" -> AnyType()),

      Map("a" -> AnyType()))
  }

  @Test def merge_two_deps_same_id_different_types() {
    testMerge(
      Map("a" -> AnyType()),
      Map("a" -> MapType()),

      Map("a" -> MapType()))
  }

  @Test def merge_two_deps_same_id_different_incompatible_types() {
    expectFailure(
      Map("a" -> StringType()),
      Map("a" -> NumberType()))
  }

  private def expectFailure(a: Map[String, CypherType], b: Map[String, CypherType]) {
    intercept[CypherTypeException](merge(a, b, a))
    intercept[CypherTypeException](merge(a, b, a))
  }

  private def testMerge(a: Map[String, CypherType], b: Map[String, CypherType], expected: Map[String, CypherType]) {
    merge(a, b, expected)
    merge(b, a, expected)
  }

  val e = new TestExpression

  private def merge(a: Map[String, CypherType], b: Map[String, CypherType], expected: Map[String, CypherType]) {
    val r = e.exposedMergeDeps(a, b)
    if (r != expected) {
      fail("""
Merged:
    %s with
    %s

     Got: %s
Expected: %s""".format(a, b, r, expected))
    }
  }
}

class TestExpression extends Expression {
  protected def compute(v1: Map[String, Any]): Any = null

  def declareDependencies(expectedType: CypherType): Seq[Identifier] = null

  def deps: Map[String, CypherType] = null

  def filter(f: (Expression) => Boolean): Seq[Expression] = null

  val identifier: Identifier = null

  def rewrite(f: (Expression) => Expression): Expression = null

  def exposedMergeDeps(a: Map[String, CypherType], b: Map[String, CypherType]): Map[String, CypherType] = mergeDeps(a, b)

  /*This is a declaration of the identifiers that this particular expression expects to
  find in the symboltable to be able to run successfully.*/
  def identifierDependencies(expectedType: CypherType) = Map()
}
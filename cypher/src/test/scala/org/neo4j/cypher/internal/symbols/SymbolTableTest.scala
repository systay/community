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
package org.neo4j.cypher.internal.symbols

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.neo4j.cypher.{CypherTypeException, SyntaxException}
import org.scalatest.Assertions
import org.neo4j.cypher.internal.commands.expressions.{Expression, Add}
import collection.Map

class SymbolTableTest extends JUnitSuite {
  @Test def givenSymbolTableWithIdentifierWhenAskForExistingThenReturnIdentifier() {
    val symbols = new SymbolTable(Identifier("x", AnyType()))
    symbols.assertHas(Identifier("x", AnyType()))
  }

  @Test(expected = classOf[SyntaxException]) def givenEmptySymbolTableWhenAskForNonExistingThenThrows() {
    val symbols = new SymbolTable()
    symbols.assertHas(Identifier("x", AnyType()))
  }

  @Test(expected = classOf[CypherTypeException]) def givenSymbolTableWithStringIdentifierWhenAskForIterableThenThrows() {
    val symbols = new SymbolTable(Identifier("x", StringType()))
    symbols.assertHas(Identifier("x", NumberType()))
  }

  @Test def givenSymbolTableWithIntegerIdentifierWhenAskForNumberThenReturn() {
    val symbols = new SymbolTable(Identifier("x", IntegerType()))
    symbols.assertHas(Identifier("x", NumberType()))
  }

  @Test def givenSymbolTableWithIterableOfStringWhenAskForIterableOfAnyThenReturn() {
    val symbols = new SymbolTable(Identifier("x", new IterableType(StringType())))
    symbols.assertHas(Identifier("x", new IterableType(AnyType())))
  }

  @Test def givenSymbolTableWithStringIdentifierWhenMergedWithNumberIdentifierThenContainsBoth() {
    val symbols = new SymbolTable(Identifier("x", StringType()))
    val newSymbol = symbols.add(Identifier("y", NumberType()))

    newSymbol.assertHas(Identifier("x", StringType()))
    newSymbol.assertHas(Identifier("y", NumberType()))
  }

  @Test(expected = classOf[SyntaxException]) def shouldNotBeAbleToCreateASymbolTableWithClashingNames() {
    new SymbolTable(Identifier("x", StringType()), Identifier("x", RelationshipType()))
  }


  @Test def filteringThroughShouldWork() {
    assert(getPercolatedIdentifier(NodeType(), AnyType()) === NodeType())
    assert(getPercolatedIdentifier(AnyIterableType(), AnyType()) === AnyIterableType())
    assert(getPercolatedIdentifier(NodeType(), NodeType()) === NodeType())
  }

  private def getPercolatedIdentifier(scopeType: CypherType, symbolType: CypherType): CypherType = new SymbolTable(Identifier("x", scopeType)).actualIdentifier(Identifier("x", symbolType)).typ

}

class SymbolTable2Test extends Assertions {
  @Test def anytype_is_ok() {
    //given
    val s = createSymbols("p" -> PathType())

    //then
    assert(s.evaluateType("p", AnyType()) === PathType())
  }

  @Test def missing_identifier() {
    //given
    val s = createSymbols()

    //then
    intercept[CypherTypeException](s.evaluateType("p", AnyType()))
  }

  @Test def identifier_with_wrong_type() {
    //given
    val symbolTable = createSymbols("x" -> StringType())

    //then
    intercept[CypherTypeException](symbolTable.evaluateType("x", NumberType()))
  }

  @Test def identifier_with_type_not_specific_enough() {
    //given
    val symbolTable = createSymbols("x" -> MapType())

    //then
    intercept[CypherTypeException](symbolTable.evaluateType("x", RelationshipType()))
  }

  @Test def adding_string_with_string_gives_string_type() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(StringType()), new FakeExpression(StringType()))

    //when
    val returnType = exp.evaluateType(AnyType(), symbolTable)

    //then
    assert(returnType === StringType())
  }

  @Test def adding_number_with_number_gives_number_type() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(NumberType()), new FakeExpression(NumberType()))

    //when
    val returnType = exp.evaluateType(AnyType(), symbolTable)

    //then
    assert(returnType === NumberType())
  }

  @Test def adding_to_string_collection() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(new IterableType(StringType())), new FakeExpression(StringType()))

    //when
    val returnType = exp.evaluateType(AnyType(), symbolTable)

    //then
    assert(returnType === new IterableType(StringType()))
  }

  @Test def covariance() {
    //given
    val actual = new IterableType(NodeType())
    val expected = new IterableType(MapType())

    //then
    assert(expected.isAssignableFrom(actual))
  }


  def createSymbols(elems: (String, CypherType)*): SymbolTable2 = {
    new SymbolTable2(elems.toMap)
  }
}

class FakeExpression(typ: CypherType) extends Expression {
  def identifierDependencies(expectedType: CypherType): Map[String, CypherType] = null

  protected def compute(v1: Map[String, Any]): Any = null

  val identifier: Identifier = Identifier("fake", typ)

  def declareDependencies(expectedType: CypherType): Seq[Identifier] = null

  def rewrite(f: (Expression) => Expression): Expression = null

  def filter(f: (Expression) => Boolean): Seq[Expression] = null

  def calculateType(symbols: SymbolTable2) = typ
}

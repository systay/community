/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.commands._
import org.junit.Assert._
import org.neo4j.graphdb.Direction
import org.scalatest.junit.JUnitSuite
import parser.{ConsoleCypherParser, CypherParser}
import org.junit.Test
import org.junit.Ignore
import org.scalatest.Assertions

class CypherParserTest extends JUnitSuite with Assertions {
  @Test def shouldParseEasiestPossibleQuery() {
    val q = Query.
      start(NodeById("s", 1)).
      returns(ValueReturnItem(EntityValue("s")))
    testQuery("select s from s = NODE(1)", q)
  }

  @Test def sourceIsAnIndex() {
    testQuery(
      """select a from a = node:index(key = "value")""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def sourceIsAnNonParsedIndexQuery() {
    testQuery(
      """select a from a = node:index("key:value")""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:value"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Ignore
  @Test def sourceIsParsedAdvancedLuceneQuery() {
    testQuery(
      """select a from a = node:index(key="value" AND otherKey="otherValue")""",
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" AND otherKey:\"otherValue\""))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Ignore
  @Test def parsedOrIdxQuery() {
    testQuery(
      """select  a from a = node:index(key="value" or otherKey="otherValue") """,
      Query.
        start(NodeByIndexQuery("a", "index", Literal("key:\"value\" OR otherKey:\"otherValue\""))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldParseEasiestPossibleRelationshipQuery() {
    testQuery(
      "select s from s = relationship(1)",
      Query.
        start(RelationshipById("s", 1)).
        returns(ValueReturnItem(EntityValue("s"))))
  }

  @Test def shouldParseEasiestPossibleRelationshipQueryShort() {
    testQuery(
      "select s from s = rel(1) ",
      Query.
        start(RelationshipById("s", 1)).
        returns(ValueReturnItem(EntityValue("s"))))
  }

  @Test def sourceIsARelationshipIndex() {
    testQuery(
      """select a from a = rel:index(key = "value")""",
      Query.
        start(RelationshipByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }


  @Test def keywordsShouldBeCaseInsensitive() {
    testQuery(
      "SELECT s FROM s = NODE(1)",
      Query.
        start(NodeById("s", 1)).
        returns(ValueReturnItem(EntityValue("s"))))
  }

  @Test def shouldParseMultipleNodes() {
    testQuery(
      "select s from s = NODE(1,2,3)",
      Query.
        start(NodeById("s", 1, 2, 3)).
        returns(ValueReturnItem(EntityValue("s"))))
  }

  @Test def shouldParseMultipleInputs() {
    testQuery(
      "select a,b from a = node(1), b = NODE(2)",
      Query.
        start(NodeById("a", 1), NodeById("b", 2)).
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def shouldFilterOnProp() {
    testQuery(
      "select a from a = NODE(1) where a.name = \"andres\"",
      Query.
        start(NodeById("a", 1)).
        where(Equals(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldFilterOnPropWithDecimals() {
    testQuery(
      "select a from a = node(1) where a.extractReturnItems = 3.1415",
      Query.
        start(NodeById("a", 1)).
        where(Equals(PropertyValue("a", "extractReturnItems"), Literal(3.1415))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleNot() {
    testQuery(
      "select a from a = node(1) where not(a.name = \"andres\")",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(PropertyValue("a", "name"), Literal("andres")))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleNotEqualTo() {
    testQuery(
      "select a from a = node(1) where a.name <> \"andres\"",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(PropertyValue("a", "name"), Literal("andres")))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleLessThan() {
    testQuery(
      "select a from a = node(1) where a.name < \"andres\"",
      Query.
        start(NodeById("a", 1)).
        where(LessThan(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleGreaterThan() {
    testQuery(
      "select a from a = node(1) where a.name > \"andres\"",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThan(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleLessThanOrEqual() {
    testQuery(
      "select a from a = node(1) where a.name <= \"andres\"",
      Query.
        start(NodeById("a", 1)).
        where(LessThanOrEqual(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleRegularComparison() {
    testQuery(
      """select a from a = node(1) where "Andres" =~ /And.*/""",
      Query.
        start(NodeById("a", 1)).
        where(RegularExpression(Literal("Andres"), Literal("And.*"))).
        returns(ValueReturnItem(EntityValue("a")))
    )
  }

  @Test def shouldHandleGreaterThanOrEqual() {
    testQuery(
      """select a from a = node(1) where a.name >= "andres"""",
      Query.
        start(NodeById("a", 1)).
        where(GreaterThanOrEqual(PropertyValue("a", "name"), Literal("andres"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }


  @Test def booleanLiterals() {
    testQuery(
      """select a from a = node(1) where true = false""",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(true), Literal(false))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldFilterOnNumericProp() {
    testQuery(
      """select a from a = NODE(1) where 35 = a.age""",
      Query.
        start(NodeById("a", 1)).
        where(Equals(Literal(35), PropertyValue("a", "age"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }


  @Test def shouldCreateNotEqualsQuery() {
    testQuery(
      """select a from a = NODE(1) where 35 != a.age""",
      Query.
        start(NodeById("a", 1)).
        where(Not(Equals(Literal(35), PropertyValue("a", "age")))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def multipleFilters() {
    testQuery(
      """select a from a = NODE(1) where a.name = "andres" or a.name = "mattias"""",
      Query.
        start(NodeById("a", 1)).
        where(Or(
        Equals(PropertyValue("a", "name"), Literal("andres")),
        Equals(PropertyValue("a", "name"), Literal("mattias")))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def relatedTo() {
    testQuery(
      """select a, b from a = NODE(1) pattern a -[:KNOWS]-> (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def relatedToWithoutRelType() {
    testQuery(
      """select a, b from a = NODE(1) pattern a --> (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    testQuery(
      """select r from a = NODE(1) pattern a -[r]-> (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", None, Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("r"))))
  }

  @Test def relatedToTheOtherWay() {
    testQuery(
      """select a, b from a = NODE(1) pattern a <-[:KNOWS]- (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.INCOMING, false)).
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def shouldOutputVariables() {
    testQuery(
      """select a.name from a = NODE(1)""",
      Query.
        start(NodeById("a", 1)).
        returns(ValueReturnItem(PropertyValue("a", "name"))))
  }

  @Test def shouldHandleAndClauses() {
    testQuery(
      """select a.name from a = NODE(1) where a.name = "andres" and a.lastname = "taylor"""",
      Query.
        start(NodeById("a", 1)).
        where(And(
        Equals(PropertyValue("a", "name"), Literal("andres")),
        Equals(PropertyValue("a", "lastname"), Literal("taylor")))).
        returns(ValueReturnItem(PropertyValue("a", "name"))))
  }

  @Test def relatedToWithRelationOutput() {
    testQuery(
      """select rel from a = NODE(1) pattern a -[rel:KNOWS]-> (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "rel", Some("KNOWS"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("rel"))))
  }

  @Test def relatedToWithoutEndName() {
    testQuery(
      """select a from a = NODE(1) pattern a -[:MARRIED]-> ()""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "  UNNAMED1", "  UNNAMED2", Some("MARRIED"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def relatedInTwoSteps() {
    testQuery(
      """select c from a = NODE(1) pattern a -[:KNOWS]-> b -[:FRIEND]-> (c)""",
      Query.
        start(NodeById("a", 1)).
        matches(
        RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, false),
        RelatedTo("b", "c", "  UNNAMED2", Some("FRIEND"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("c")))
    )
  }

  @Test def djangoRelationshipType() {
    testQuery(
      """select c from a = NODE(1) pattern a -[:`<<KNOWS>>`]-> b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("<<KNOWS>>"), Direction.OUTGOING, false)).
        returns(ValueReturnItem(EntityValue("c"))))
  }

  @Test def countTheNumberOfHits() {
    testQuery(
      """select a, b, count(*) from a = NODE(1) pattern a --> b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(CountStar()).
        columns("a", "b", "count(*)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def distinct() {
    testQuery(
      """select distinct a, b from a = NODE(1) pattern a --> b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation().
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def sumTheAgesOfPeople() {
    testQuery(
      """select a, b, sum(a.age) from a = NODE(1) pattern a --> b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Sum(PropertyValue("a", "age")))).
        columns("a", "b", "sum(a.age)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def avgTheAgesOfPeople() {
    testQuery(
      """select a, b, avg(a.age) from a = NODE(1) pattern a --> b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Avg(PropertyValue("a", "age")))).
        columns("a", "b", "avg(a.age)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def minTheAgesOfPeople() {
    testQuery(
      """select a, b, min(a.age) from a = NODE(1) pattern (a) --> b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Min(PropertyValue("a", "age")))).
        columns("a", "b", "min(a.age)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def maxTheAgesOfPeople() {
    testQuery(
      """select a, b, max(a.age) from a = NODE(1) pattern a --> b""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)).
        aggregation(ValueAggregationItem(Max(( PropertyValue("a", "age") )))).
        columns("a", "b", "max(a.age)").
        returns(ValueReturnItem(EntityValue("a")), ValueReturnItem(EntityValue("b"))))
  }

  @Test def singleColumnSorting() {
    testQuery(
      """select a from a = NODE(1) order by a.name """,
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueReturnItem(PropertyValue("a", "name")), true)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def sortOnAggregatedColumn() {
    testQuery(
      """select a from a = NODE(1) order by avg(a.name)""",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueAggregationItem(Avg(PropertyValue("a", "name"))), true)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleTwoSortColumns() {
    testQuery(
      """select a from a = NODE(1) order by a.name, a.age""",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(ValueReturnItem(PropertyValue("a", "name")), true),
        SortItem(ValueReturnItem(PropertyValue("a", "age")), true)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleTwoSortColumnsAscending() {
    testQuery(
      """select a from a = NODE(1) order by a.name ASCENDING, a.age ASC""",
      Query.
        start(NodeById("a", 1)).
        orderBy(
        SortItem(ValueReturnItem(PropertyValue("a", "name")), true),
        SortItem(ValueReturnItem(PropertyValue("a", "age")), true)).
        returns(ValueReturnItem(EntityValue("a"))))

  }

  @Test def orderByDescending() {
    testQuery(
      """select a from a = NODE(1) order by a.name DESCENDING""",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueReturnItem(PropertyValue("a", "name")), false)).
        returns(ValueReturnItem(EntityValue("a"))))

  }

  @Test def orderByDesc() {
    testQuery(
      """select a from a = NODE(1) order by a.name desc""",
      Query.
        start(NodeById("a", 1)).
        orderBy(SortItem(ValueReturnItem(PropertyValue("a", "name")), false)).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def nullableProperty() {
    testQuery(
      """select a.name? from a = NODE(1)""",
      Query.
        start(NodeById("a", 1)).
        returns(ValueReturnItem(NullablePropertyValue("a", "name"))))
  }

  @Test def nestedBooleanOperatorsAndParentesis() {
    testQuery(
      """select n from n = NODE(1,2,3) where (n.animal = "monkey" and n.food = "banana") or (n.animal = "cow" and n.food="grass")""",
      Query.
        start(NodeById("n", 1, 2, 3)).
        where(Or(
        And(
          Equals(PropertyValue("n", "animal"), Literal("monkey")),
          Equals(PropertyValue("n", "food"), Literal("banana"))),
        And(
          Equals(PropertyValue("n", "animal"), Literal("cow")),
          Equals(PropertyValue("n", "food"), Literal("grass"))))).
        returns(ValueReturnItem(EntityValue("n"))))
  }

  @Test def limit5() {
    testQuery(
      """select n from n=NODE(1) limit 5""",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        returns(ValueReturnItem(EntityValue("n"))))
  }

  @Test def skip5() {
    testQuery(
      """select n from n=NODE(1) skip 5""",
      Query.
        start(NodeById("n", 1)).
        skip(5).
        returns(ValueReturnItem(EntityValue("n"))))
  }

  @Test def skip5limit5() {
    testQuery(
      """select n from n=NODE(1) skip 5 limit 5""",
      Query.
        start(NodeById("n", 1)).
        limit(5).
        skip(5).
        returns(ValueReturnItem(EntityValue("n"))))
  }

  @Test def relationshipType() {
    testQuery(
      """select r from n=NODE(1) pattern n-[r]->(x) where type(r) = "something"""",
      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
        where(Equals(RelationshipTypeValue(EntityValue("r")), Literal("something"))).
        returns(ValueReturnItem(EntityValue("r"))))
  }

  @Test def pathLength() {
    testQuery(
      """select p from n=NODE(1) pattern p=(n-->x) where LENGTH(p) = 10""",
      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        where(Equals(ArrayLengthValue(EntityValue("p")), Literal(10.0))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def relationshipTypeOut() {
    testQuery(
      """select type(r) from n=NODE(1) pattern n-[r]->(x)""",

      Query.
        start(NodeById("n", 1)).
        matches(RelatedTo("n", "x", "r", None, Direction.OUTGOING, false)).
        returns(ValueReturnItem(RelationshipTypeValue(EntityValue("r")))))
  }

  @Test def relationshipsFromPathOutput() {
    testQuery(
      """select relationships(p) from n=NODE(1) pattern p=n-->x""",

      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        returns(ValueReturnItem(PathRelationshipsValue(EntityValue("p")))))
  }

  @Test def relationshipsFromPathInWhere() {
    testQuery(
      """select p from n=NODE(1) pattern p=n-->x where length(rels(p))=1""",

      Query.
        start(NodeById("n", 1)).
        namedPaths(NamedPath("p", RelatedTo("n", "x", "  UNNAMED1", None, Direction.OUTGOING, false))).
        where(Equals(ArrayLengthValue(PathRelationshipsValue(EntityValue("p"))), Literal(1)))
        returns ( ValueReturnItem(EntityValue("p")) ))
  }

  @Test def countNonNullValues() {
    testQuery(
      """select a, count(a) from a = NODE(1)""",
      Query.
        start(NodeById("a", 1)).
        aggregation(ValueAggregationItem(Count(EntityValue("a")))).
        columns("a", "count(a)").
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleIdBothInReturnAndWhere() {
    testQuery(
      """select id(a) from a = NODE(1) where id(a) = 0""",
      Query.
        start(NodeById("a", 1)).
        where(Equals(IdValue(EntityValue("a")), Literal(0)))
        returns ( ValueReturnItem(IdValue(EntityValue("a"))) ))
  }

  @Test def shouldBeAbleToHandleStringLiteralsWithApostrophe() {
    testQuery(
      """select a from a = node:index(key = 'value')""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("value"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def shouldHandleQuotationsInsideApostrophes() {
    testQuery(
      """select a from a = node:index(key = 'val"ue')""",
      Query.
        start(NodeByIndex("a", "index", Literal("key"), Literal("val\"ue"))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def simplePathExample() {
    testQuery(
      """select a from a = node(0) pattern p = ( a-->b )""",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false))).
        returns(ValueReturnItem(EntityValue("a"))))
  }

  @Test def threeStepsPath() {
    testQuery(
      """select a from a = node(0) pattern p = ( a-->b-->c )""",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p",
                             RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false),
                             RelatedTo("b", "c", "  UNNAMED2", None, Direction.OUTGOING, false)
      ))
        returns ( ValueReturnItem(EntityValue("a")) ))
  }

  @Test def pathsShouldBePossibleWithoutParenthesis() {
    testQuery(
      """select a from a = node(0) pattern p = a-->b""",
      Query.
        start(NodeById("a", 0)).
        namedPaths(NamedPath("p", RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false)))
        returns ( ValueReturnItem(EntityValue("a")) ))
  }

  @Test def variableLengthPath() {
    testQuery("""select x from a=node(0) pattern a -[:knows*1..3]-> x""",
              Query.
                start(NodeById("a", 0)).
                matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(1), Some(3), "knows", Direction.OUTGOING)).
                returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def fixedVarLengthPath() {
    testQuery("""select x from a=node(0) pattern a -[*3]-> x""",
              Query.
                start(NodeById("a", 0)).
                matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(3), Some(3), None, Direction.OUTGOING, None,
                                           false)).
                returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def variableLengthPathWithoutMinDepth() {
    testQuery("""select x from a=node(0) pattern a -[:knows*..3]-> x""",
              Query.
                start(NodeById("a", 0)).
                matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, Some(3), "knows", Direction.OUTGOING)).
                returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def variableLengthPathWithRelationshipIdentifier() {
    testQuery("""select x from a=node(0) pattern a -[r:knows*2..]-> x""",
              Query.
                start(NodeById("a", 0)).
                matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, Some("knows"), Direction.OUTGOING,
                                           Some("r"), false)).
                returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def variableLengthPathWithoutMaxDepth() {
    testQuery("""select x from a=node(0) pattern a -[:knows*2..]-> x""",
              Query.
                start(NodeById("a", 0)).
                matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", Some(2), None, "knows", Direction.OUTGOING)).
                returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def unboundVariableLengthPath() {
    testQuery("""select x from a=node(0) pattern a -[:knows*]-> x""",
              Query.
                start(NodeById("a", 0)).
                matches(VarLengthRelatedTo("  UNNAMED1", "a", "x", None, None, "knows", Direction.OUTGOING)).
                returns(ValueReturnItem(EntityValue("x")))
    )
  }

  @Test def optionalRelationship() {
    testQuery(
      """select b from a = node(1) pattern a -[?]-> (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, true)).
        returns(ValueReturnItem(EntityValue("b"))))
  }

  @Test def optionalTypedRelationship() {
    testQuery(
      """select b from a = node(1) pattern a -[?:KNOWS]-> (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "  UNNAMED1", Some("KNOWS"), Direction.OUTGOING, true)).
        returns(ValueReturnItem(EntityValue("b"))))
  }

  @Test def optionalTypedAndNamedRelationship() {
    testQuery(
      """select b from a = node(1) pattern a -[r?:KNOWS]-> (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", Some("KNOWS"), Direction.OUTGOING, true)).
        returns(ValueReturnItem(EntityValue("b"))))
  }

  @Test def optionalNamedRelationship() {
    testQuery(
      """select b from a = node(1) pattern a -[r?]-> (b)""",
      Query.
        start(NodeById("a", 1)).
        matches(RelatedTo("a", "b", "r", None, Direction.OUTGOING, true)).
        returns(ValueReturnItem(EntityValue("b"))))
  }

  @Test def testOnAllNodesInAPath() {
    testQuery(
      """select  b from a = node(1) pattern p = a --> b --> c where ALL(n in NODES(p) : n.name = "Andres") """,
      Query.
        start(NodeById("a", 1)).
        namedPaths(
        NamedPath("p",
                  RelatedTo("a", "b", "  UNNAMED1", None, Direction.OUTGOING, false),
                  RelatedTo("b", "c", "  UNNAMED2", None, Direction.OUTGOING, false))).
        where(AllInSeq(PathNodesValue(EntityValue("p")), "n", Equals(PropertyValue("n", "name"), Literal("Andres"))))
        returns ( ValueReturnItem(EntityValue("b")) ))
  }

  @Test def testAny() {
    testQuery(
      """select  b from a = node(1) where ANY(x in NODES(p): x.name = "Andres") """,
      Query.
        start(NodeById("a", 1)).
        where(AnyInSeq(PathNodesValue(EntityValue("p")), "x", Equals(PropertyValue("x", "name"), Literal("Andres"))))
        returns ( ValueReturnItem(EntityValue("b")) ))
  }

  @Test def testNone() {
    testQuery(
      """select  b from a = node(1) where none(x in nodes(p) : x.name = "Andres") """,
      Query.
        start(NodeById("a", 1)).
        where(NoneInSeq(PathNodesValue(EntityValue("p")), "x", Equals(PropertyValue("x", "name"), Literal("Andres"))))
        returns ( ValueReturnItem(EntityValue("b")) ))
  }

  @Test def testSingle() {
    testQuery(
      """select  b from a = node(1) where single(x in NODES(p): x.name = "Andres") """,
      Query.
        start(NodeById("a", 1)).
        where(SingleInSeq(PathNodesValue(EntityValue("p")), "x", Equals(PropertyValue("x", "name"),
                                                                        Literal("Andres"))))
        returns ( ValueReturnItem(EntityValue("b")) ))
  }

  @Test def testParamAsStartNode() {
    testQuery(
      """select  pA from pA = node({a}) """,
      Query.
        start(NodeById("pA", ParameterValue("a"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testNumericParamNameAsStartNode() {
    testQuery(
      """select  pA from pA = node({0}) """,
      Query.
        start(NodeById("pA", ParameterValue("0"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForWhereLiteral() {
    testQuery(
      """select  pA from pA = node(1) where pA.name = {name} """,
      Query.
        start(NodeById("pA", 1)).
        where(Equals(PropertyValue("pA", "name"), ParameterValue("name")))
        returns ( ValueReturnItem(EntityValue("pA")) ))
  }

  @Test def testParamForIndexKey() {
    testQuery(
      """select  pA from pA = node:idx({key} = "Value") """,
      Query.
        start(NodeByIndex("pA", "idx", ParameterValue("key"), Literal("Value"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForIndexValue() {
    testQuery(
      """select  pA from pA = node:idx(key = {Value}) """,
      Query.
        start(NodeByIndex("pA", "idx", Literal("key"), ParameterValue("Value"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForIndexQuery() {
    testQuery(
      """select  pA from pA = node:idx({query}) """,
      Query.
        start(NodeByIndexQuery("pA", "idx", ParameterValue("query"))).
        returns(ValueReturnItem(EntityValue("pA"))))
  }

  @Test def testParamForSkip() {
    testQuery(
      """select pA from pA = node(0) skip {skipper}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        returns ( ValueReturnItem(EntityValue("pA")) ))
  }

  @Test def testParamForLimit() {
    testQuery(
      """select pA from pA = node(0) limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        limit("stop")
        returns ( ValueReturnItem(EntityValue("pA")) ))
  }

  @Test def testParamForLimitAndSkip() {
    testQuery(
      """select pA from pA = node(0) skip {skipper} limit {stop}""",
      Query.
        start(NodeById("pA", 0)).
        skip("skipper")
        limit ( "stop" )
        returns ( ValueReturnItem(EntityValue("pA")) ))
  }

  @Test def testParamForRegex() {
    testQuery(
      """select pA from pA = node(0) where pA.name =~ {regex} """,
      Query.
        start(NodeById("pA", 0)).
        where(RegularExpression(PropertyValue("pA","name"), ParameterValue("regex")))
        returns ( ValueReturnItem(EntityValue("pA")) ))
  }

  @Test def testShortestPath() {
    testQuery(
      """select  p from a=node(0), b=node(1) pattern p = shortestPath( a-->b ) """,
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", None, Direction.OUTGOING, Some(1), false))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testShortestPathWithMaxDepth() {
    testQuery(
      """select  p from a=node(0), b=node(1) pattern p = shortestPath( a-[*..6]->b ) """,
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", None, Direction.OUTGOING, Some(6), false))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testShortestPathWithType() {
    testQuery(
      """select  p from a=node(0), b=node(1) pattern p = shortestPath( a-[:KNOWS*..6]->b ) """,
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", Some("KNOWS"), Direction.OUTGOING, Some(6),
                                               false))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testShortestPathBiDirectional() {
    testQuery(
      """select  p from a=node(0), b=node(1) pattern p = shortestPath( a-[*..6]-b ) """,
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", None, Direction.BOTH, Some(6), false))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testShortestPathOptional() {
    testQuery(
      """select  p from a=node(0), b=node(1) pattern p = shortestPath( a-[?*..6]-b ) """,
      Query.
        start(NodeById("a", 0), NodeById("b", 1)).
        namedPaths(NamedPath("p", ShortestPath("  UNNAMED2", "a", "b", None, Direction.BOTH, Some(6), true))).
        returns(ValueReturnItem(EntityValue("p"))))
  }

  @Test def testForNull() {
    testQuery(
      """select  a from a=node(0) where a is null """,
      Query.
        start(NodeById("a", 0)).
        where(IsNull(EntityValue("a")))
        returns ( ValueReturnItem(EntityValue("a")) ))
  }

  @Test def testForNotNull() {
    testQuery(
      """select  a from a=node(0) where a is not null """,
      Query.
        start(NodeById("a", 0)).
        where(Not(IsNull(EntityValue("a"))))
        returns ( ValueReturnItem(EntityValue("a")) ))
  }

  @Test def testCountDistinct() {
    testQuery(
      """select count(distinct a) from a=node(0)""",
      Query.
        start(NodeById("a", 0)).
        aggregation(ValueAggregationItem(Distinct(Count(EntityValue("a")), EntityValue("a")))).
        columns("count(distinct a)")
        returns())
  }

  @Test def consoleModeParserShouldOutputNullableProperties() {
    val query = """select a.name from a = node(1)"""
    val parser = new ConsoleCypherParser()
    val executionTree = parser.parse(query)

    assertEquals(
      Query.
        start(NodeById("a", 1)).
        returns(ValueReturnItem(NullablePropertyValue("a", "name"))),
      executionTree)
  }

  def testQuery(query: String, expectedQuery: Query) {
    val parser = new CypherParser()

    try {
      val ast = parser.parse(query)

      assert(expectedQuery === ast)
    } catch {
      case x => {
        println(x)
        throw new Exception(query + "\n\n" + x.getMessage)
      }
    }
  }


}

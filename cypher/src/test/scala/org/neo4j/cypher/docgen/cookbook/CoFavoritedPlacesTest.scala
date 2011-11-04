package org.neo4j.cypher.docgen.cookbook

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

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.junit.Ignore


class CoFavoritedPlacesTest extends DocumentingTestBase {
  def graphDescription = List("Joe favorite CoffeeShop1", 
      "Joe favorite SaunaX", 
      "Joe favorite MelsPlace",
      "Jill favorite CoffeeShop1", 
      "Jill favorite MelsPlace", 
      "CoffeeShop2 tagged Cool",
      "CoffeeShop1 tagged Cool",
      "CoffeeShop1 tagged Cosy",
      "CoffeeShop3 tagged Cosy",
      "MelsPlace tagged Cosy",
      "MelsPlace tagged Cool",
      "Jill favorite CoffeShop2")

  def section = "cookbook"

  @Test def coFavoritedPlaces() {
    testQuery(
      title = "Co-Favorited Places - Users Who Like x Also Like y",
      text = """Find places that people also like who favorite this place:

* Determine who has favorited place x.
* What else have they favorited that is not place x.""",
      queryText = """SELECT stuff.name, count(*)
      FROM place=node:node_auto_index(name = "CoffeeShop1")
      		PATTERN place<-[:favorite]-person-[:favorite]->stuff
      		ORDER BY count(*) DESC, stuff.name""",
      returns = "The list of places that are favorited by people that favorited the start place.",
      (p) => assertEquals(List(Map("stuff.name" -> "MelsPlace", "count(*)" -> 2),
          Map("stuff.name" -> "CoffeShop2", "count(*)" -> 1),
          Map("stuff.name" -> "SaunaX", "count(*)" -> 1)),p.toList))
  } 
  
  @Test def coTaggedPlaces() {
    testQuery(
      title = "Co-Tagged Places - Places Related through Tags",
      text = """Find places that are tagged with the same tags:

* Determine the tags for place x.
* What else is tagged the same as x that is not x.""",
      queryText = """SELECT otherPlace.name, collect(tag.name)
          FROM place=node:node_auto_index(name = "CoffeeShop1")
      		PATTERN place-[:tagged]->tag<-[:tagged]-otherPlace
      		ORDER By otherPlace.name desc""",
      returns = "The list of possible friends ranked by them liking similar stuff that are not yet friends.",
      (p) => {
        println(p.dumpToString())
        assertEquals(List(Map("otherPlace.name" -> "MelsPlace", "collect(tag.name)" -> List("Cool", "Cosy")),
                Map("otherPlace.name" -> "CoffeeShop3", "collect(tag.name)" -> List("Cosy")),
                Map("otherPlace.name" -> "CoffeeShop2", "collect(tag.name)" -> List("Cool"))),p.toList)
      })
  } 
}
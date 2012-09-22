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
package org.neo4j.cypher.internal.pipes.nestedHashJoin

import collection.mutable
import collection.mutable.ListBuffer

/*
This class does the actual probing - given a probe map, it will traverse an input and find any matchers between the two
 */
class Probe(input: Iterator[Map[String, Any]],
            map: mutable.Map[Seq[Any], ListBuffer[Map[String, Any]]],
            keyExtractor: (Map[String, Any] => Seq[Any])) extends Iterator[Map[String, Any]] {

  private val buffer: Iterator[Map[String, Any]] = probe()

  def probe() = {
    //the probe map
    val m = input.next()
    val k = keyExtractor(m)

    val iter: Iterable[Map[String, Any]] = map.getOrElse(k, None)

    iter.view.map(m => m ++ m).toIterator
  }

  def hasNext = buffer.hasNext

  def next() = {
    val n = buffer.next()
    if (buffer.isEmpty && input.nonEmpty) {
      probe()
    }
    n
  }
}
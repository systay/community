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
import org.neo4j.helpers.ThisShouldNotHappenError

class HashJoinContext(private var inputA: Iterator[Map[String, Any]],
                      private var inputB: Iterator[Map[String, Any]],
                      keyExtractor: (Map[String, Any] => Seq[Any]),
                      maxSize: Int) {

  //This is the next match to emit
  var emit: Option[Map[String, Any]] = None

  //These are the two probe tables
  var mapA: mutable.Map[Seq[Any], ListBuffer[Map[String, Any]]] = mutable.Map.empty
  var mapB: mutable.Map[Seq[Any], ListBuffer[Map[String, Any]]] = mutable.Map.empty

  //If we have zipped two probe tables, this is where we'll find the buffer from that result
  var zipBuffer: Iterator[Map[String, Any]] = None.toIterator

  var probe: Probe = null
  var nextDropProbe: Option[Probe] = None
  var dropProbe: Probe = null


  def atLeastOneInputEmpty = !inputA.hasNext || !inputB.hasNext

  def atLeastOneInputUnfinished = inputA.hasNext || inputB.hasNext

  def exactlyOneInputUnfinished = (inputA.hasNext && !inputB.hasNext) || (!inputA.hasNext && inputB.hasNext)

  def inputsEmpty = !(inputA.hasNext || inputB.hasNext)

  def bothInputsUnfinished = inputA.hasNext && inputB.hasNext

  def reachedMax = mapA.size >= maxSize

  def zipEmpty = !zipBuffer.hasNext


  /*
 This method creates the view of the zipped tables
  */
  def buildZipBuffer() {
    zipBuffer = mapA.view.flatMap {
      case (key, bufferA: ListBuffer[Map[String, Any]]) =>
        val bufferB = mapB.getOrElse(key, new ListBuffer[Map[String, Any]]())

        bufferA.flatMap {
          case m => bufferB.map(mapB => m ++ mapB)
        }

    }.toIterator
  }

  def buildMaps() {
    addMapToBuffer(mapA, inputA.next())
    addMapToBuffer(mapB, inputB.next())
  }

  def prepareProbe() {
    val (newProbeInput, newProbeMap) = if (inputA.hasNext) {
      mapA = mutable.Map.empty
      (inputA, mapB)
    } else {
      mapB = mutable.Map.empty
      (inputB, mapA)
    }

    probe = new Probe(newProbeInput, newProbeMap, keyExtractor)
  }

  def prepareDropProbe() {
    //Probe for the A probe map
    val probeA = {
      val map = mapA

      val (a, b) = inputB.duplicate
      val (a1,a2) = a.duplicate
      val asdfasdf = a2.toList

      val input = a1

      inputB = b
      mapA = mutable.Map.empty
      new Probe(input, map, keyExtractor)
    }

    //Probe for the A probe map
    val probeB = {
      val map = mapB

      val (a, b) = inputA.duplicate
      val input = a
      inputA = b

      mapB = mutable.Map.empty
      new Probe(input, map, keyExtractor)
    }

    dropProbe = probeA
    nextDropProbe = Some(probeB)
  }

  def consumeZipBuffer() {
    emit = Some(zipBuffer.next())
  }

  def consumeProbeBuffer() {
    emit = Some(probe.next())
  }

  def consumeDropProbeBuffer() {
    emit = Some(dropProbe.next())
  }

  private def addMapToBuffer(mapBuffer: mutable.Map[Seq[Any], ListBuffer[Map[String, Any]]], map: Map[String, Any]) {
    val keyA = keyExtractor(map)
    val buffer = mapBuffer.getOrElseUpdate(keyA, new ListBuffer[Map[String, Any]]())
    buffer += map
  }

  def getNextResult: Map[String, Any] = emit match {
    case Some(result) =>
      emit = None
      result

    case None =>
      throw new ThisShouldNotHappenError("Andrés", "Tried advancing before a value was ready")
  }

  def switchDropProbe() {
    dropProbe = nextDropProbe match {
      case Some(p) =>
        nextDropProbe = None
        p
      case None    =>
        throw new ThisShouldNotHappenError("Andrés", "Can not get next drop probe because there is none")
    }
  }

}

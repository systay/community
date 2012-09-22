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

import org.neo4j.helpers.ThisShouldNotHappenError

/*
A naive join between two input streams is to simply iterate over one of them, and for each element compare it
with every single element in the other stream.

A hash join builds up a probe table from one of the inputs, and compares it against every single element in the
other input. The trade off is the memory used by the probe table, which makes it important to pick the smaller
of the two inputs as the probe input.

A nested hash join combines both strategies - we divide up the input sets in probe buckets and compare each bucket
from the left input with every element on the other input.

If we reach the end of one of the inputs, let's call it X, we first zip together the two probe tables, and then we
follow the normal hash join strategy and probe the remainder of the other input Y with the probe table built from X.

If both inputs end at the same time, we zip the probe tables and we are done.

Finally, if we reach the probe table maximum, we first zip together both tables, and then we probe the remainder
of X, (X') with the Y table, and Y' with the X table. When that is done, we start the nested hash join over again,
using X' and Y' as inputs.

Here is the states and transitions:

CREATE 	build = {state:"Build"},
		zip = {state:"Zip Maps"},
		done = {state:"Done"},
		probe = {state:"Probe"},
		end_probe = {state:"End Probe"},
		build-[:transition {if:"at least one input depleted"}]->zip,
		build-[:transition {if:"reached map size"}]->zip,
		zip-[:transition {if:"both inputs depleted"}]->done,
		zip-[:transition {if:"one input unfinished and one depleted"}]->end_probe,
		end_probe-[:transition {if:"both inputs depleted"}]->done,
		probe-[:transition {if:"other input unfinished"}]->probe,
		probe-[:transition {if:"both inputs probed"}]->build
 */
class NestedHashJoin(inputA: Traversable[Map[String, Any]],
                     inputB: Traversable[Map[String, Any]],
                     keyExtractor: (Map[String, Any] => Seq[Any]),
                     maxSize: Int) extends Iterator[Map[String, Any]] {
  import States._

  val context: HashJoinContext = new HashJoinContext(inputA.toIterator, inputB.toIterator, keyExtractor, maxSize)
  var state: State = Build

  /*
 Initial step. Here we do some work, to prepare for the first match.
 If there are no matches, we might go over the inputs in this first step.
  */
  step()

  def process(state: State): State = state match {
    case Build if context.atLeastOneInputEmpty =>
      context.buildZipBuffer()
      MapZip

    case Build if context.reachedMax =>
      context.buildZipBuffer()
      MapZip

    case Build =>
      context.buildMaps()
      Build

    case MapZip if !context.zipEmpty =>
      context.consumeZipBuffer()
      MapZip

    case MapZip if context.zipEmpty && context.exactlyOneInputUnfinished =>
      context.prepareProbe()
      Probe

    case MapZip if context.zipEmpty && context.bothInputsUnfinished =>
      context.prepareDropProbe()
      DropProbe

    case DropProbe if context.dropProbe.isEmpty && context.nextDropProbe.nonEmpty =>
      context.switchDropProbe()
      DropProbe

    case DropProbe if context.dropProbe.isEmpty && context.nextDropProbe.isEmpty =>
      Build

    case DropProbe if context.dropProbe.nonEmpty =>
      context.consumeDropProbeBuffer()
      DropProbe

    case MapZip if !context.zipBuffer.hasNext && context.inputsEmpty =>
      Done

    case Probe if context.probe.hasNext =>
      context.consumeProbeBuffer()
      Probe

    case Probe if context.probe.isEmpty =>
      Done

    case _ => throw new ThisShouldNotHappenError("Stefan P", "Forgot a case in the fancy hash join state machine:\nState: " + state)
  }

  /*
  We continue advancing forward until we have found the next match
   */
  def step() {
    while (state != Done && context.emit.isEmpty) {
      state = process(state)
    }
  }

  def hasNext = context.emit.isDefined

  def next() = {
    val result = context.getNextResult

    if (state != Done)
      step()

    result
  }
}

object States extends Enumeration {
  type State = Value

  val
  /*This marks the build stage - we are building the probe tables, but staying under max size*/
  Build,

  /*In this stage, we are zipping together two probe tables*/
  MapZip,

  /*Here we are doing the normal hash join - we have a single probe table,
  and we are consuming the other input*/
  Probe,

  /*
  When we are drop probing, we'll probe first with the probe table for input a, and then with the table for probe b
   */
  DropProbe,

  /*
  We're done. After reaching this stage, the state machine is finished
   */
  Done = Value
}
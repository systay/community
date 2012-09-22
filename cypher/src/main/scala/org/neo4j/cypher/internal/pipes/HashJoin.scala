package org.neo4j.cypher.internal.pipes

import org.neo4j.helpers.ThisShouldNotHappenError
import collection.mutable
import collection.mutable.ListBuffer

class HashJoin(inputA: Traversable[Map[String, Any]],
               inputB: Traversable[Map[String, Any]],
               keyExtractor: (Map[String, Any] => Seq[Any]),
               maxSize: Int) extends Iterator[Map[String, Any]] {

  import States._
  import Messages._

  class Context(var inputA: Iterator[Map[String, Any]], var inputB: Iterator[Map[String, Any]]) {
    var message = Start

    var emit: Option[Map[String, Any]] = None

    var mapA: mutable.Map[Seq[Any], ListBuffer[Map[String, Any]]] = mutable.Map.empty
    var mapB: mutable.Map[Seq[Any], ListBuffer[Map[String, Any]]] = mutable.Map.empty
    var zipBuffer: Iterator[Map[String, Any]] = None.toIterator

    var probeInput: Iterator[Map[String, Any]] = None.toIterator
    var probeMap: mutable.Map[Seq[Any], ListBuffer[Map[String, Any]]] = mutable.Map.empty
    var probeBuffer: Iterator[Map[String, Any]] = None.toIterator
    var size: Int = 0


    def atLeastOneInputEmpty = !inputA.hasNext || !inputB.hasNext

    def atLeastOneInputNotEmpty = inputA.hasNext || inputB.hasNext

    def inputsEmpty = !(inputA.hasNext || inputB.hasNext)

    def reachedMax = size >= maxSize
  }

  def process(state: State): State = state match {
    case Build if context.atLeastOneInputEmpty =>
      prepareZip()
      MapZip

    case Build if context.reachedMax =>
      prepareZip()
      MapZip

    case Build =>
      buildMaps()
      Build

    case MapZip if context.zipBuffer.hasNext =>
      consumeZipBuffer()
      MapZip

    case MapZip if !context.zipBuffer.hasNext && context.atLeastOneInputNotEmpty =>
      prepareProbe()
      Probe

    case MapZip if !context.zipBuffer.hasNext && context.inputsEmpty =>
      Done

    case Probe if context.probeBuffer.hasNext =>
      consumeProbeBuffer()
      Probe

    case Probe if context.probeInput.hasNext =>
      probe()
      Probe

    case Probe if !context.probeInput.hasNext =>
      Done

    case _ => throw new ThisShouldNotHappenError("Stefan P", "Forgot a case in the fancy hash join state machine:\nState: " + state)
  }

  private def buildMaps() {
    val nextA = context.inputA.next()
    addMapToBuffer(context.mapA, nextA)

    val nextB = context.inputB.next()
    addMapToBuffer(context.mapB, nextB)

    context.size = context.size + 1
  }

  private def prepareProbe() {
    /*
    First we check which input that is depleted, and use it's hash table. We then drop the other map, and the empty input.
     */

    val (probeInput, probeMap) = if (context.inputA.hasNext) {
      context.mapA = mutable.Map.empty
      (context.inputA, context.mapB)
    } else {
      context.mapB = mutable.Map.empty
      (context.inputB, context.mapA)
    }

    context.probeMap = probeMap
    context.probeInput = probeInput
  }

  private def consumeZipBuffer() {
    context.emit = Some(context.zipBuffer.next())
  }

  private def consumeProbeBuffer() {
    context.emit = Some(context.probeBuffer.next())
  }

  private def probe() {
    val map = context.probeInput.next()
    val key = keyExtractor(map)

    val iter: Iterable[Map[String, Any]] = context.probeMap.getOrElse(key, None)

    context.probeBuffer = iter.view.map(m => m ++ map).toIterator
  }

  private def prepareZip() {
    context.zipBuffer = context.mapA.view.flatMap {
      case (key, bufferA: ListBuffer[Map[String, Any]]) =>
        val bufferB = context.mapB.getOrElse(key, new ListBuffer[Map[String, Any]]())

        bufferA.flatMap {
          case mapA => bufferB.map(mapB => mapA ++ mapB)
        }

    }.toIterator
  }


  private def addMapToBuffer(mapBuffer: mutable.Map[Seq[Any], ListBuffer[Map[String, Any]]],
                             map: Map[String, Any]) {
    val keyA = keyExtractor(map)
    val buffer = mapBuffer.getOrElseUpdate(keyA, new ListBuffer[Map[String, Any]]())
    buffer += map
  }

  val context: Context = new Context(inputA.toIterator, inputB.toIterator)
  var state: State = Build

  def step() {
    while (state != Done && context.emit.isEmpty) {
      state = process(state)
    }
  }

  def hasNext = context.emit.isDefined

  def next() = {
    val result = getNextResult()

    if (state != Done)
      step()

    result
  }

  private def getNextResult(): Map[String, Any] = {
    val result = context.emit.get
    context.emit = None
    result
  }

  // INITIAL STEP
  step()
}


object States extends Enumeration {
  type State = Value

  val Build,
  MapZip,
  Probe,
  DropProbe,
  Done = Value
}

object Messages extends Enumeration {
  type Message = Value

  val Start, OneEmpty, TwoEmpty, TooBig = Value
}

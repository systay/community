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
package org.neo4j.cypher.internal.pipes

import aggregation.AggregationFunction
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.commands.expressions.{Expression, AggregationExpression}
import collection.mutable.{Map => MutableMap}
import org.neo4j.cypher.internal.symbols.AnyType
import org.neo4j.cypher.internal.symbols.Identifier

// Eager aggregation means that this pipe will eagerly load the whole resulting sub graphs before starting
// to emit aggregated results.
// Cypher is lazy until it can't - this pipe will eagerly load the full match
class EagerAggregationPipe(source: Pipe, val keyExpressions: Map[String, Expression], aggregations: Map[String, AggregationExpression])
  extends PipeWithSource(source) {
  def oldKeyExpressions = keyExpressions.values.toSeq

  val symbols: SymbolTable = createSymbols()
  val symbols2: SymbolTable2 = createSymbols2()

  def dependencies: Seq[Identifier] = oldKeyExpressions.flatMap(_.dependencies(AnyType())) ++
                                      aggregations.flatMap(_._2.dependencies(AnyType()))

  private def createSymbols() = {
    val map: Seq[String] = oldKeyExpressions.map(_.identifier.name)
    val keySymbols: SymbolTable = source.symbols.filter(map: _*)
    val aggregatedColumns: Seq[Identifier] = aggregations.map(_._2.identifier).toSeq

    keySymbols.add(aggregatedColumns: _*)
  }

  private def createSymbols2() = {
    val typeExtractor: ((String, Expression)) => (String, CypherType) = {
      case (id, exp) => id -> exp.evaluateType(AnyType(), source.symbols2)
    }

    val keyIdentifiers = keyExpressions.map(typeExtractor)
    val aggrIdentifiers = aggregations.map(typeExtractor)

    new SymbolTable2(keyIdentifiers ++ aggrIdentifiers)
  }

  def createResults(state: QueryState): Traversable[ExecutionContext] = {
    // This is the temporary storage used while the aggregation is going on
    val result = MutableMap[NiceHasher, (ExecutionContext, Seq[AggregationFunction])]()
    val keyNames: Seq[String] = keyExpressions.map(_._1).toSeq
    val aggregationNames: Seq[String] = aggregations.map(_._1).toSeq

    source.createResults(state).foreach(ctx => {
      val groupValues: NiceHasher = new NiceHasher(keyNames.map(ctx(_)))
      val (_, functions) = result.getOrElseUpdate(groupValues, (ctx, aggregations.map(_._2.createAggregationFunction).toSeq))
      functions.foreach(func => func(ctx))
    })

    val resultContext = if (result.isEmpty && keyNames.isEmpty) {
      createEmptyResult(aggregationNames)
    } else result.map {
      case (key, (ctx, aggregator)) => createResults(keyNames, key, aggregationNames, aggregator, ctx)
    }

    resultContext
  }


  def createResults(keyNames: scala.Seq[String], key: NiceHasher, aggregationNames: scala.Seq[String], aggregator: scala.Seq[AggregationFunction], ctx: ExecutionContext): ExecutionContext = {
    val newMap = MutableMaps.create

    //add key values
    (keyNames zip key.original).foreach(newMap += _)

    //add aggregated values
    (aggregationNames zip aggregator.map(_.result)).foreach(newMap += _)

    ctx.newFrom(newMap)
  }

  private def createEmptyResult(aggregationNames: Seq[String]): Traversable[ExecutionContext] = {
    val newMap = MutableMaps.create
    val aggregationNamesAndFunctions = aggregationNames zip aggregations.map(_._2.createAggregationFunction.result)
    aggregationNamesAndFunctions.toMap
      .foreach {
      case (name, zeroValue) => newMap += name -> zeroValue
    }
    Traversable(ExecutionContext(newMap))
  }

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "EagerAggregation( keys: [" + oldKeyExpressions.map(_.identifier.name).mkString(", ") + "], aggregates: [" + aggregations.mkString(", ") + "])"

  def deps = mergeDeps(oldKeyExpressions.map(_.identifierDependencies(AnyType())) ++ aggregations.map(_._2.identifierDependencies(AnyType())))
}
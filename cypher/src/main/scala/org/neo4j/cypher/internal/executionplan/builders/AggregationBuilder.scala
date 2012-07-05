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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.pipes.{Pipe, ExtractPipe, EagerAggregationPipe}
import org.neo4j.cypher.internal.executionplan.{ExecutionPlanInProgress, PlanBuilder}
import org.neo4j.cypher.internal.commands.expressions.{CachedExpression, AggregationExpression, Expression}

class AggregationBuilder extends PlanBuilder with ExpressionExtractor {
  def apply(plan: ExecutionPlanInProgress) = {
    val (keyExpressionsToExtract, _) = getExpressions(plan)

    val newPlan = ExtractBuilder.extractIfNecessary(plan, keyExpressionsToExtract)

    val x = getExpressions(newPlan)
    val namedKeyExpressions: Map[String, Expression] = x._1
    val aggregationExpressions: Seq[AggregationExpression] = x._2

    val pipe = new EagerAggregationPipe(newPlan.pipe, namedKeyExpressions, aggregationExpressions)

    val query = newPlan.query

    val allReturnExpressions = query.returns.flatMap(_.token.expressions(pipe.symbols2))

    val notKeyAndNotAggregate = allReturnExpressions.filterNot(namedKeyExpressions.values.toSeq.contains)

    val resultPipe = extractAggregatedValues(notKeyAndNotAggregate, pipe)

    val resultQ = query.copy(
      aggregation = query.aggregation.map(_.solve),
      aggregateQuery = query.aggregateQuery.solve,
      extracted = true
    ).rewrite(removeAggregates)

    newPlan.copy(query = resultQ, pipe = resultPipe)
  }


  def extractAggregatedValues(notKeyAndNotAggregate: Seq[(String, Expression)], pipe: EagerAggregationPipe): Pipe = if (notKeyAndNotAggregate.isEmpty) pipe
  else {

    val rewritten: Map[String, Expression] = notKeyAndNotAggregate.map {
      case (name,expression) => name -> expression.rewrite(removeAggregates)
    }.toMap

    new ExtractPipe(pipe, rewritten)
  }

  private def removeAggregates(e: Expression) = e match {
    case e: AggregationExpression => CachedExpression(e.identifier.name, e.identifier)
    case x                        => x
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query

    q.aggregateQuery.token &&
    q.aggregateQuery.unsolved &&
    q.readyToAggregate

  }

  def priority: Int = PlanBuilder.Aggregation
}

trait ExpressionExtractor {
  def getExpressions(plan: ExecutionPlanInProgress): (Map[String, Expression], Seq[AggregationExpression]) = {
    val keys = plan.query.returns.flatMap(_.token.expressions(plan.pipe.symbols2)).filterNot(tuple => tuple._2.containsAggregate)

    val returnAggregates: Seq[AggregationExpression] = plan.query.aggregation.map(_.token)
    val sortAggregates: Seq[AggregationExpression] = plan.query.sort.filter(_.token.expression.isInstanceOf[AggregationExpression]).map(_.token.expression.asInstanceOf[AggregationExpression])

    val aggregates: Seq[AggregationExpression] = sortAggregates ++ returnAggregates

    (keys.toMap, aggregates)
  }
}
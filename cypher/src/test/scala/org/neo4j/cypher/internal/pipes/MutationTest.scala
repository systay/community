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

import org.junit.{Before, Test}
import org.neo4j.cypher.internal.mutation.DeleteEntityAction
import org.neo4j.cypher.{StatsAssertions, CypherTypeException, ExecutionEngineHelper}
import collection.mutable.{Map => MutableMap}
import org.neo4j.graphdb.{Node, NotFoundException}
import org.neo4j.cypher.internal.symbols.{SymbolTable, CypherType, NodeType}
import org.neo4j.cypher.internal.commands.{CreateRelationshipStartItem, CreateNodeStartItem}
import org.neo4j.cypher.internal.commands.expressions.{Expression, Literal}


class MutationTest extends ExecutionEngineHelper with StatsAssertions {

  var state: QueryState = null

  @Before
  def init() {
    state = QueryState.forTest(graph)
  }

  @Test
  def create_node() {
    val start = new NullPipe
    val txBegin = new TransactionStartPipe(start, graph)
    val createNode = new ExecuteUpdateCommandsPipe(txBegin, graph, Seq(CreateNodeStartItem("n", Map("name" -> Literal("Andres")))))
    val commitPipe = new CommitPipe(createNode, graph)

    commitPipe.createResults(state)

    val n = graph.getNodeById(1)
    assert(n.getProperty("name") === "Andres")
    assertStats(state, createdNode = 1, setProperty = 1)
  }

  @Test
  def join_existing_transaction_and_rollback() {
    val tx = graph.beginTx()
    val start = new NullPipe
    val txBegin = new TransactionStartPipe(start, graph)
    val createNode = new ExecuteUpdateCommandsPipe(txBegin, graph, Seq(CreateNodeStartItem("n", Map("name" -> Literal("Andres")))))
    val commitPipe = new CommitPipe(createNode, graph)

    commitPipe.createResults(state)

    tx.failure()
    tx.finish()

    intercept[NotFoundException](graph.getNodeById(1))
  }

  @Test
  def join_existing_transaction_and_commit() {
    val tx = graph.beginTx()
    val start = new NullPipe
    val txBegin = new TransactionStartPipe(start, graph)
    val createNode = new ExecuteUpdateCommandsPipe(txBegin, graph, Seq(CreateNodeStartItem("n", Map("name" -> Literal("Andres")))))
    val commitPipe = new CommitPipe(createNode, graph)

    commitPipe.createResults(state)

    tx.success()
    tx.finish()

    val n = graph.getNodeById(1)
    assert(n.getProperty("name") === "Andres")
  }

  private def getNode(key: String, n: Node) = InjectValue(n, NodeType())

  @Test
  def create_rel() {
    val a = createNode()
    val b = createNode()

    val createRel = CreateRelationshipStartItem("r", (getNode("a", a), Map()), (getNode("b", b), Map()), "REL", Map("I" -> Literal("was here")))

    val startPipe = new NullPipe
    val txBeginPipe = new TransactionStartPipe(startPipe, graph)
    val createNodePipe = new ExecuteUpdateCommandsPipe(txBeginPipe, graph, Seq(createRel))
    val commitPipe = new CommitPipe(createNodePipe, graph)

    val results: List[MutableMap[String, Any]] = commitPipe.createResults(state).map(ctx => ctx.m).toList

    val r = graph.getRelationshipById(0)
    assert(r.getProperty("I") === "was here")
    assert(results === List(Map("r" -> r)))
    assertStats(state, createdRelationship = 1, setProperty = 1)
  }

  @Test
  def throw_exception_if_wrong_stuff_to_delete() {
    val createRel = DeleteEntityAction(Literal("some text"))

    intercept[CypherTypeException](createRel.exec(ExecutionContext.empty, state))
  }

  @Test
  def delete_node() {
    val a: Node = createNode()
    val node_id: Long = a.getId
    val deleteCommand = DeleteEntityAction(getNode("a", a))

    val startPipe = new NullPipe
    val txBeginPipe = new TransactionStartPipe(startPipe, graph)
    val createNodePipe = new ExecuteUpdateCommandsPipe(txBeginPipe, graph, Seq(deleteCommand))
    val commitPipe = new CommitPipe(createNodePipe, graph)

    commitPipe.createResults(state).toList

    intercept[NotFoundException](graph.getNodeById(node_id))
  }
}

case class InjectValue(value: Any, typ: CypherType) extends Expression {
  def apply(v1: ExecutionContext) = value

  def filter(f: (Expression) => Boolean) = Seq(this)

  def rewrite(f: (Expression) => Expression) = this

  def calculateType(symbols: SymbolTable): CypherType = typ

  def symbolTableDependencies = Set()
}
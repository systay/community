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

import org.neo4j.kernel.Traversal
import org.neo4j.cypher.SyntaxException
import java.lang.String
import org.neo4j.graphdb.{Expander, DynamicRelationshipType, Node}
import collection.Seq
import org.neo4j.cypher.internal.symbols.{NodeType, Identifier, PathType}
import org.neo4j.cypher.internal.commands.{ReturnItem, ShortestPath}
import collection.mutable.Map
/**
 * Shortest pipe inserts a single shortest path between two already found nodes
 *
 * It's also the base class for all shortest paths
 */
abstract class ShortestPathPipe(source: Pipe, ast: ShortestPath) extends PipeWithSource(source) {
  def startName = ast.start
  def endName = ast.end
  def relType = ast.relTypes
  def dir = ast.dir
  def maxDepth = ast.maxDepth
  def optional = ast.optional
  def pathName = ast.pathName
  def returnItems: Seq[ReturnItem] = Seq()

  def createResults(state: QueryState) = source.createResults(state).flatMap(ctx => {
    val (start, end) = getStartAndEnd(ctx)
    val expander = createExpander
    val depth = maxDepth.getOrElse(15)

    findResult(expander, start, end, depth, ctx)
  })

  private def getStartAndEnd[U](m: Map[String, Any]): (Node, Node) = {
    val err = (n: String) => throw new SyntaxException("To find a shortest path, both ends of the path need to be provided. Couldn't find `" + n + "`")

    val start = m.getOrElse(startName, err(startName)).asInstanceOf[Node]
    val end = m.getOrElse(endName, err(endName)).asInstanceOf[Node]
    (start, end)
  }

  private lazy val createExpander: Expander = if (relType.isEmpty) {
    Traversal.expanderForAllTypes(dir)
  } else {
    relType.foldLeft(Traversal.emptyExpander()){case(e,t)=>e.add(DynamicRelationshipType.withName(t), dir)}
  }

  def dependencies: Seq[Identifier] = Seq(Identifier(startName, NodeType()), Identifier(endName, NodeType()))

  protected def findResult[U](expander: Expander, start: Node, end: Node, depth: Int, m: ExecutionContext): Traversable[ExecutionContext]

  val symbols = source.symbols.add(Identifier(pathName, PathType()))

  override def executionPlan(): String = source.executionPlan() + "\r\n" + "ShortestPath(" + ast + ")"
}

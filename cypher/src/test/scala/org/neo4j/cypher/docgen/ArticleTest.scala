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
package org.neo4j.cypher.docgen

import org.neo4j.graphdb.index.Index
import org.junit.Test
import scala.collection.JavaConverters._
import java.io.{File, PrintWriter, ByteArrayOutputStream}
import org.neo4j.graphdb._
import org.neo4j.visualization.graphviz.{AsciiDocStyle, GraphvizWriter, GraphStyle}
import org.neo4j.walk.Walker
import org.neo4j.visualization.asciidoc.AsciidocHelper
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds
import org.neo4j.cypher.javacompat.GraphImpl
import org.neo4j.cypher.{CypherParser, ExecutionResult, ExecutionEngine}
import org.neo4j.test.{GeoffService, ImpermanentGraphDatabase, TestGraphDatabaseFactory, GraphDescription}
import org.scalatest.Assertions

/*
Use this base class for tests that are more flowing text with queries intersected in the middle of the text.
 */
abstract class ArticleTest extends Assertions with DocumentationHelper {

  var db: GraphDatabaseService = null
  val parser: CypherParser = new CypherParser
  var engine: ExecutionEngine = null
  var nodes: Map[String, Long] = null
  var nodeIndex: Index[Node] = null
  var relIndex: Index[Relationship] = null
  val properties: Map[String, Map[String, Any]] = Map()
  var generateConsole: Boolean = true
  var generateInitialGraphForConsole: Boolean = true
  def title:String
  def section:String

  def graphDescription: List[String]

  def indexProps: List[String] = List()

  protected def getGraphvizStyle: GraphStyle = {
    AsciiDocStyle.withAutomaticRelationshipTypeColors()
  }

  private def emitGraphviz(fileName: String): String = {
    val out = new ByteArrayOutputStream()
    val writer = new GraphvizWriter(getGraphvizStyle)
    writer.emit(out, Walker.fullGraph(db))

    return """
_Graph_

["dot", %s.svg", "neoviz"]
----
%s
----

""".format(fileName, out)
  }

  def dumpGraphViz(graphViz: PrintWriter, fileName: String) {
    val foo = emitGraphviz(fileName)
    graphViz.write(foo)
    graphViz.flush()
    graphViz.close()
  }

  def executeQuery(queryText: String): ExecutionResult = engine.execute(replaceNodeIds(queryText))

  def replaceNodeIds(_query: String):String={
    var query = _query
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    query
  }

  def testWithoutDocs(queryText: String, assertions: (ExecutionResult => Unit)*): (ExecutionResult, String) = {
    var query = queryText
    nodes.keySet.foreach((key) => query = query.replace("%" + key + "%", node(key).getId.toString))
    val result = engine.execute(query)
    assertions.foreach(_.apply(result))
    (result, query)
  }

  def indexProperties[T <: PropertyContainer](n: T, index: Index[T]) {
    indexProps.foreach((property) => {
      if (n.hasProperty(property)) {
        val value = n.getProperty(property)
        index.add(n, property, value)
      }
    })
  }

  def node(name: String): Node = db.getNodeById(nodes.getOrElse(name, throw new NotFoundException(name)))

  def rel(id: Long): Relationship = db.getRelationshipById(id)


  def text: String

  def expandQuery(query: String) = {

    val querySnippet = AsciidocHelper.createCypherSnippet(query)
    val resultSnippet = AsciidocHelper.createQueryResultSnippet(executeQuery(query).dumpToString())

    """_Query_

%s

.Result
%s
%s
""".format(querySnippet, resultSnippet, consoleSnippet(replaceNodeIds(query)))
  }


  def consoleSnippet(query:String): String = {
    if (generateConsole) {
      val create = if (generateInitialGraphForConsole) new GeoffService(db).toGeoff() else "start n=node(*) match n-[r]->() delete n, r;"
      """.Try this query live
[console]
----
%s

%s
----
""".format(create, query)
    } else ""
  }

  def header = "[[%s-%s]]".format(section.toLowerCase, title.toLowerCase)

  @Test
  def produceDocumentation() {
    val db = init()
    try {
      val (dir: File, writer: PrintWriter) = createWriter(title, section)

      val queries = ("(?s)#(.*?)#".r findAllIn text).toList

      var producedText = text
      queries.foreach {
        query => {
          val q = query.replaceAll("#", "")
          producedText = producedText.replace(query, expandQuery(q))
        }
      }

      writer.println(header)
      writer.println(producedText)
      writer.close()
    } finally {
      db.shutdown()
    }
  }

  private def init() = {
    db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase()

    db.asInstanceOf[ImpermanentGraphDatabase].cleanContent(false)

    db.inTx(() => {
      nodeIndex = db.index().forNodes("nodes")
      relIndex = db.index().forRelationships("rels")
      val g = new GraphImpl(graphDescription.toArray[String])
      val description = GraphDescription.create(g)

      nodes = description.create(db).asScala.map {
        case (name, node) => name -> node.getId
      }.toMap

      db.getAllNodes.asScala.foreach((n) => {
        indexProperties(n, nodeIndex)
        n.getRelationships(Direction.OUTGOING).asScala.foreach(indexProperties(_, relIndex))
      })

      properties.foreach((n) => {
        val nod = node(n._1)
        n._2.foreach((kv) => nod.setProperty(kv._1, kv._2))
      })
    })
    engine = new ExecutionEngine(db)
    db
  }
}





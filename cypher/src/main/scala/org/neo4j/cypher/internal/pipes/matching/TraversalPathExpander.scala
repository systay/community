package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.graphdb.{Relationship, Path, PathExpander}
import org.neo4j.graphdb.traversal.BranchState
import java.lang.{Iterable => JIterable}
import collection.JavaConverters._

class TraversalPathExpander extends PathExpander[Option[ExpanderStep]] {
  def expand(path: Path, state: BranchState[Option[ExpanderStep]]): JIterable[Relationship] = {

    val result: Iterable[Relationship] = state.getState match {
      case None => Seq()

      case Some(step) =>
        val node = path.endNode()
        val rels: Iterable[Relationship] = step.expand(node)
        state.setState(step.next)
        rels
    }

    result.asJava
  }

  def reverse(): PathExpander[Option[ExpanderStep]] = this
}

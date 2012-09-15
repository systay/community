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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.graphdb._
import org.neo4j.cypher.internal.pipes.QueryState
import traversal._
import java.lang.{Iterable => JIterable}
import collection.JavaConverters._
import org.neo4j.kernel.{StandardBranchCollisionDetector, Uniqueness, Traversal}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.kernel.impl.traversal.BranchCollisionPolicy

class TraversalMatcher(path: Seq[PatternRelationship], start: () => Iterable[Node], end: () => Iterable[Node]) {

  lazy val steps = makeSteps()
  lazy val reversedSteps = steps.reverse()

  val initialStartStep = new InitialStateFactory[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(steps)
  }

  val initialEndStep = new InitialStateFactory[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(reversedSteps)
  }
  val baseTraversal: TraversalDescription = Traversal.traversal(Uniqueness.RELATIONSHIP_PATH)
  val collisionDetector = new StepCollisionDetector

  def matchThatShit(state: QueryState): Iterable[Path] = {
    val result: JIterable[Path] = Traversal.bidirectionalTraversal()
      .startSide(baseTraversal.expand(new OurPathExpander, initialStartStep))
      .endSide(baseTraversal.expand(new OurPathExpander, initialEndStep))
      .collisionPolicy(collisionDetector)
      .traverse(start().asJava, end().asJava)

    result.asScala
  }

  class StepCollisionDetector extends StandardBranchCollisionDetector(null) with BranchCollisionPolicy {
    override def evaluate(branch: TraversalBranch, direction: Direction) = {
      val r = super.evaluate(branch, direction)
      if (false) println(Traversal.simplePathToString(branch, "name") + " for " + direction)
      r
    }


    override def includePath(path: Path, startPath: TraversalBranch, endPath: TraversalBranch): Boolean = {
      val s = startPath.state().asInstanceOf[Option[ExpanderStep]]
      val e = endPath.state().asInstanceOf[Option[ExpanderStep]]

      val result = (s, e) match {
        case (Some(startStep), Some(endStep)) => endStep.id == startStep.id + 1
        case (Some(x), None)                  => startPath.length() == 0
        case (None, Some(x))                  => endPath.length() == 0
        case _                                => throw new ThisShouldNotHappenError("Mattias", "Impossible. Cannot be.")
      }

      if (result) {
        startPath.prune()
        endPath.prune()
      }

      result
    }

    def create(evaluator: Evaluator) = this
  }

  class OurPathExpander extends PathExpander[Option[ExpanderStep]] {
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

  //Creates the steps given a path of PatternRelationships
  type foldType = (Int, Option[ExpanderStep])

  private def makeSteps(): ExpanderStep = {
    path.reverse.foldLeft[foldType]((0, None)) {
      case (last, pr) =>
        val relTypes = pr.relTypes.map(DynamicRelationshipType.withName)
        val id = last._1
        (id + 1, Some(ExpanderStep(id, relTypes, pr.dir, last._2)))
    }._2.get
  }
}
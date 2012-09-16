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

class TraversalMatcher(steps: ExpanderStep, start: () => Iterable[Node], end: () => Iterable[Node]) {

  lazy val reversedSteps = steps.reverse()

  val initialStartStep = new InitialStateFactory[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(steps)
  }

  val initialEndStep = new InitialStateFactory[Option[ExpanderStep]] {
    def initialState(path: Path): Option[ExpanderStep] = Some(reversedSteps)
  }
  val baseTraversal: TraversalDescription = Traversal.traversal(Uniqueness.RELATIONSHIP_PATH)
  val collisionDetector = new StepCollisionDetector

  def findMatchingPaths(state: QueryState): Iterable[Path] = {
    val result: JIterable[Path] = Traversal.bidirectionalTraversal()
      .startSide(baseTraversal.expand(new TraversalPathExpander, initialStartStep))
      .endSide(baseTraversal.expand(new TraversalPathExpander, initialEndStep))
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
}
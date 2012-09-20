package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.graphdb.{Direction, PropertyContainer}
import org.neo4j.cypher.internal.symbols.{RelationshipType, NodeType, SymbolTable}
import org.neo4j.cypher.internal.commands.{True, Predicate}
import org.neo4j.graphdb.DynamicRelationshipType._
import org.neo4j.cypher.internal.pipes.matching.ExpanderStep
import scala.Some


sealed abstract class Trail {
  def pathDescription: Seq[String]
  def start: String
  def end: String
  def size: Int
  def toSteps(id: Int): Option[ExpanderStep]
  override def toString: String = pathDescription.toString()
  def decompose(p: Seq[PropertyContainer]): Map[String, Any] = decompose(p, Map.empty)._2
  protected[builders] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]): (Seq[PropertyContainer], Map[String, Any])
  def symbols(table: SymbolTable): SymbolTable
  def contains(target: String): Boolean
  def predicates:Seq[Predicate]
}

final case class BoundPoint(name: String) extends Trail {
  def end = name
  def pathDescription = Seq(name)
  def start = name
  def size = 0
  def toSteps(id: Int) = None
  protected[builders] def decompose(p: Seq[PropertyContainer], r: Map[String, Any]) = {
    assert(p.size == 1, "Expected a path with a single node in it")
    (p.tail, r ++ Map(name -> p.head))
  }
  def symbols(table: SymbolTable): SymbolTable = table.add(name, NodeType())
  def contains(target: String): Boolean = target == name
  def predicates = Seq.empty
}

final case class WrappingTrail(s: Trail,
                               dir: Direction,
                               rel: String,
                               typ: Seq[String],
                               end: String,
                               candPredicates: Seq[Predicate]) extends Trail {

  val relPred  = candPredicates.find(createFinder(rel))
  val nodePred = candPredicates.find(createFinder(end))

  private def containsSingle(set: Set[String], elem: String) = set.size == 1 && set.head == elem

  private def createFinder(elem: String): (Predicate => Boolean) =
    (pred: Predicate) => containsSingle(pred.symbolTableDependencies, elem)

  def start = s.start

  def pathDescription = s.pathDescription ++ Seq(rel, end)

  def toSteps(id: Int) = {
    val types = typ.map(withName(_))
    val steps = s.toSteps(id + 1)
    val relPredicate = relPred.getOrElse(True())
    val nodePredicate = nodePred.getOrElse(True())

    Some(ExpanderStep(id, types, dir, steps, relPredicate, nodePredicate))
  }

  def size = s.size + 1

  protected[builders] def decompose(p: Seq[PropertyContainer], m: Map[String, Any]) = {
    val r = p.tail.head
    val n = p.head
    val newMap = m + (rel -> r) + (end -> n)
    s.decompose(p.tail.tail, newMap)
  }

  def symbols(table: SymbolTable): SymbolTable = s.symbols(table).add(end, NodeType()).add(rel, RelationshipType())

  def contains(target: String): Boolean = s.contains(target) || target == end

  def predicates = nodePred.toSeq ++ relPred.toSeq ++ s.predicates
}

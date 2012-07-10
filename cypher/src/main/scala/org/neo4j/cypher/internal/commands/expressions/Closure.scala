package org.neo4j.cypher.internal.commands.expressions

trait Closure {
  def symbolTableDependencies(collection:TypeSafe, closure:TypeSafe, id:String) = {
    val predicateDeps: Set[String] = closure.symbolTableDependencies - id
    val collectionDeps: Set[String] = collection.symbolTableDependencies
    predicateDeps ++ collectionDeps
  }
}
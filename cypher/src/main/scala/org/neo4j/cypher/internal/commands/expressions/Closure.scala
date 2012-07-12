package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.symbols.TypeSafe

trait Closure {
  def symbolTableDependencies(collection:TypeSafe, closure:TypeSafe, id:String) = {
    val predicateDeps: Set[String] = closure.symbolTableDependencies - id
    val collectionDeps: Set[String] = collection.symbolTableDependencies
    predicateDeps ++ collectionDeps
  }
}
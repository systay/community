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
package org.neo4j.server.scripting;

import static java.util.Arrays.asList;

import java.util.HashSet;
import java.util.Set;

/**
 * A set of classes that we trust unknown entities to work with. These will be accessible to users that have remote access
 * to a Neo4j database.
 *
 * Please make sure that, when you add a class to this whitelist, it does not allow any sort of side effects that could
 * be dangerous, such as accessing the file system or starting remote connections or threads, unless we clearly know
 * the effect of it.
 *
 * Assume that people using these classes will be using a Neo4j database that runs in a co-located environment, and that
 * the world will burn if someone is able to access a database they are not supposed to have access to.
 *
 * This White List should not be end-user configurable. If we let end-users set up their own whitelists, then each database
 * would have it's own "language" for extension, which will lead to massive complications in the longer term. Better
 * to have an authoritative white list that is the same for all databases.
 */
public class UserScriptClassWhiteList
{

    public static Set<String> getWhiteList()
    {
        String[] whites = {
                // START SNIPPET: sandBoxingWhiteList
                // Core API concepts
                "org.neo4j.graphdb.Path",
                "org.neo4j.graphdb.Node",
                "org.neo4j.graphdb.Relationship",
                "org.neo4j.graphdb.RelationshipType",
                "org.neo4j.graphdb.DynamicRelationshipType",
                "org.neo4j.graphdb.Lock",
                "org.neo4j.graphdb.NotFoundException",

                // Traversal concepts
                "org.neo4j.graphdb.Direction",
                "org.neo4j.graphdb.traversal.Evaluation",

                // Java Core API
                "java.lang.Object",
                "java.lang.String",
                "java.lang.Integer",
                "java.lang.Long",
                "java.lang.Float",
                "java.lang.Double",
                "java.lang.Boolean",

                // Internals needed
                "org.neo4j.kernel.impl.traversal.TraversalBranchImpl",
                "org.neo4j.kernel.impl.core.NodeProxy",
                "org.neo4j.kernel.impl.traversal.StartNodeTraversalBranch"
                // END SNIPPET: sandBoxingWhiteList
        };

        return new HashSet<String>(asList(whites));
    }

}

NEO4J_TEST_CONNECTION = """MATCH (n) RETURN count(n) as count"""

# create
NEO4J_CREATE_DOCUMENT_NODE_RETURN_ID = """
CYPHER 3.5
CREATE (node { }) RETURN ID(node) as ID"""
NEO4J_CREATE_NODE_SET_PART = "SET node.`{attr_name}` = {{`{attr_name}`}}"
NEO4J_CREATE_NODE_SET_PART_MERGE_ATTR = "SET node.`{attr_name}` = (CASE WHEN not exists(node.`{attr_name}`) THEN [{{`{attr_name}`}}] ELSE node.`{attr_name}` + {{`{attr_name}`}}  END)"
NEO4J_CREATE_NODE_MERGE_CHECK_PART = """WITH CASE WHEN check = 0 THEN (CASE  WHEN EXISTS(node.`{attr_name}`) AND node.`{attr_name}` <> {{`{attr_name}`}} THEN 1 ELSE 0 END) ELSE 1 END as check , node"""
NEO4J_CREATE_NODE_RETURN_ID = """
                                CYPHER 3.5
                                MERGE (node:{label} {{{formal_attributes}}})
                                WITH 0 as check, node
                                {merge_check_statement}
                                {set_statement}
                                RETURN ID(node) as ID, check """  # args: provType, values
NEO4J_CREATE_RELATION_RETURN_ID = """
                                CYPHER 3.5
                                MATCH
                                    (from{{`meta:identifier`:'{from_identifier}'}}),
                                    (to{{`meta:identifier`:'{to_identifier}'}})
                                MERGE
                                    (from)-[r:{relation_type} {{{formal_attributes}}}]->(to)
                                    WITH 0 as check, r as node
                                    {merge_check_statement}
                                    {set_statement}
                                RETURN
                                    ID(node) as ID, check
                                """  # args: provType, values
# get
NEO4J_GET_RECORDS_BY_PROPERTY_DICT = """
                            CYPHER 3.5 
                            MATCH (d {{{filter_dict}}} )-[r]-(x {{{filter_dict}}})
                            RETURN DISTINCT r as re
                            //Get all nodes that are alone without connections to other nodes
                            UNION
                            MATCH (a {{{filter_dict}}})
                            RETURN DISTINCT a as re
                        """
NEO4J_GET_RECORDS_TAIL_BY_FILTER = """
                            CYPHER 3.5
                            MATCH (x {{{filter_dict}}})-[r *{depth}]-(y)
                            RETURN  DISTINCT y as re
                            UNION
                            MATCH (x {{{filter_dict}}})-[r *{depth}]-(y)
                            WITH REDUCE(output = [], r IN r | output + r) AS flat
                            UNWIND flat as re
                            RETURN DISTINCT re
                        """

NEO4J_GET_BUNDLE_RECORDS = """
                            CYPHER 3.5
                            MATCH (x {`meta:identifier`: {`meta:identifier`}})-[r *1]-(y)
                            WHERE ALL (rel in r WHERE rel.`prov:type` = 'prov:bundleAssociation')
                            RETURN  DISTINCT y as re
                            UNION
                            //get all relations between the nodes
                            MATCH (origin {`meta:identifier`: {`meta:identifier`}})-[r *1]-(x)-[r_return *1]-(y)-[r_2 *1]-(origin {`meta:identifier`: {`meta:identifier`}})
                            WHERE ALL (rel in r WHERE rel.`prov:type` = 'prov:bundleAssociation')
                            AND ALL (rel in r_2 WHERE rel.`prov:type` = 'prov:bundleAssociation')
                            WITH REDUCE(output = [], r IN r_return | output + r) AS flat
                            UNWIND flat as re
                            RETURN DISTINCT re
                            //get all mentionof relations
                            UNION
                            MATCH (bundle_1 {`meta:identifier`: {`meta:identifier`}})-[r *1]-(x)-[r_return *1]-(y)-[r_2 *1]-(bundle_2)
                            WHERE ALL (rel in r WHERE rel.`prov:type` = 'prov:bundleAssociation')
                            AND ALL (rel in r_2 WHERE rel.`prov:type` = 'prov:bundleAssociation')
                            AND ALL (rel in r_return WHERE rel.`meta:prov_type` = 'prov:Mention'  and startNode(rel) = x)
                            WITH REDUCE(output = [], r IN r_return | output + r) AS flat
                            UNWIND flat as re
                            RETURN DISTINCT re

 """

NEO4J_GET_RECORD_RETURN_NODE = """
CYPHER 3.5
MATCH (node) WHERE ID(node)={record_id} RETURN node"""
NEO4J_GET_RELATION_RETURN_NODE = """
CYPHER 3.5
MATCH ()-[relation]-() WHERE ID(relation)={relation_id}  RETURN relation"""

# delete
NEO4J_DELETE__NODE_BY_ID = """
CYPHER 3.5
MATCH (x) Where ID(x) = {node_id} DETACH DELETE x """
NEO4J_DELETE_NODE_BY_PROPERTIES = """
CYPHER 3.5
MATCH (n {{{filter_dict}}}) DETACH DELETE n"""
NEO4J_DELETE_BUNDLE_NODE_BY_ID = """
CYPHER 3.5
MATCH (b) WHERE id(b)=toInt({bundle_id}) DELETE b """
NEO4J_DELETE_RELATION_BY_ID = """
CYPHER 3.5
MATCH ()-[r]-() WHERE id(r) = {relation_id} DELETE r"""

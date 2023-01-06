"""Main module."""
# import libs and load config
import os
from neo4j import (
    basic_auth,
    AsyncGraphDatabase,
    GraphDatabase
)
import json
import pandas as pd
from typing import Optional, List, Dict, Iterable
#from sqlalchemy import create_engine
import pandas as pd
#import pymysql
import configparser
from graphviz import Digraph
from tqdm import tqdm
#from pymysql import cursors
import numpy as np
import re
from typing_extensions import LiteralString
from neo4j_tools import defaults

config = configparser.ConfigParser()
config.read(defaults.config_file_path)

# set database
# mysql_user = config['MYSQL']['user']
# mysql_passwd = config['MYSQL']['password']
# mysql_host = config['MYSQL']['host']
# mysql_database = config['MYSQL']['database']
# print('Default MySQL database:', mysql_database)

neo4j_uri = config['NEO4J']['uri']
neo4j_user = config['NEO4J']['user']
neo4j_password = config['NEO4J']['password']
neo4j_import_folder = config['NEO4J']['import_folder']

# engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_passwd}@{mysql_host}/{mysql_database}?charset=utf8mb4')

# conn = pymysql.connect(user=mysql_user, passwd=mysql_passwd, host=mysql_host, database=mysql_database)
# dict_cursor = conn.cursor(cursors.DictCursor)

def get_standard_name(name: str) -> str:
    """Return standard name."""
    part_of_name = [x for x in re.findall("[A-Z]*[a-z0-9]*", name) if x]
    new_name = "_".join(part_of_name).lower()
    if re.search(r'^\d+', new_name):
        new_name = '_' + new_name
    return new_name

def get_cypher_props(props: Optional[dict]):
    """Convert dictionary to cypher compliant properties as string."""
    props_str = ''
    props_array = []
    if props:
        for k,v in props.items():
            if (isinstance(v, (str, int, list)) and v) or (isinstance(v, float) and not np.isnan(v)):
                cypher_str = f"`{k}`: " + json.dumps(v)
                props_array.append(cypher_str)
        if props_array:
            props_str = "{" + ', '.join(props_array) + "}"
    return props_str

# define Node and Edge classes
class GraphElement:
    def __init__(self, label: str, props: Optional[dict]=None):
        self.label = label
        self.props = props

    @property
    def cypher_props(self) -> str:
        return get_cypher_props(self.props)

    def __get_sql_value(self, value):
        return json.dumps(value) if isinstance(value, str) else value

    def get_where(self, prefix: str) -> Optional[str]:
        if self.props:
            return ' AND '.join([f"{prefix}.{k} = {self.__get_sql_value(v)}" for k,v in self.props.items()])

    def __str__(self):
        return f"<{self.label}: {self.cypher_props}>"


class Node(GraphElement):
    def __init__(self, label: str, props: Optional[dict]=None):
        super().__init__(label, props)

class Edge(GraphElement):
    def __init__(self, label: str, props: Optional[dict]=None):
        super().__init__(label, props)

py_neo_cast_map = {
    bool: 'toBooleanOrNull',
    float: 'toFloatOrNull',
    int: 'toIntegerOrNull'
}


class Db:
    def __init__(self,
            password = neo4j_password,
            uri = "bolt://localhost:7687",
            user = "neo4j",
            neo4j_import_folder=neo4j_import_folder):
        self._uri = uri
        self._user = user
        self._password = password
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.session = self.driver.session()
        self.import_folder = neo4j_import_folder

    def exec_data(self, cypher: LiteralString):
        r = self.session.run(cypher)
        return r.data()

    def exec_df(self, cypher: LiteralString):
        data = self.exec_data(cypher)
        if set([len(x.keys()) for x in data]) == {1,}:
            only_available_key = list(data[0].keys())[0]
            if isinstance(data[0][only_available_key], dict):
                data = [dict({'Label in Cypher': only_available_key},**x[only_available_key]) for x in data]
        return pd.DataFrame(data)

    def close(self):
        self.driver.close()

    def show_indexes(self, as_df=True):
        cypher = "SHOW INDEXES"
        return self.exec_df(cypher) if as_df else self.exec_data(cypher)

    def show_unique_constraints(self, as_df=True):
        cypher = "SHOW UNIQUE CONSTRAINTS"
        return self.exec_df(cypher) if as_df else self.exec_data(cypher)

    @property
    def nodes(self):
        cypher = "MATCH (n) RETURN n"
        return [x['n'] for x in self.exec_data(cypher)]

    def nodes_by_label(self, label:str, limit:Optional[int]=None):
        cypher_limit = f"LIMIT {limit}" if limit else ''
        cypher = f"MATCH (n:{label}) RETURN n {cypher_limit}"
        return [x['n'] for x in self.exec_data(cypher)]

    def create_node(self, node: Node):
        """Create a node with label and properties."""
        cypher = f"CREATE (n:{node.label} {node.cypher_props}) return ID(n) as id"  
        return self.exec_data(cypher)[0]['id']

    def __get_sql_value(self, value):
        return f'"' + json.dumps(value) + '"' if isinstance(value, str) else value

    def __get_where_part_by_props(self, node_name: str, props: dict):
        return ' AND '.join([f"{node_name}.{k} = {self.__get_sql_value(v)}" for k,v in props.items()])

    def create_edge(self, start_node: Node, edge:Edge, end_node: Node):
        start_where = start_node.get_where('start_node')
        end_where = end_node.get_where('end_node')
        cypher = f"""MATCH
            (start_node:{start_node.label}),
            (end_node:{end_node.label})
        WHERE {start_where} AND {end_where}
        CREATE (start_node)-[r:{edge.label} {edge.cypher_props}]->(end_node)
        RETURN ID(r) as rid"""
        return self.session.run(cypher)

    def set_props(self,node_id: int, props: dict):
        # TODO: Implement!
        cypher = """SET
            e.property1 = $value1,
            e.property2 = $value2"""
        return self.exec_data(cypher)[0]

    def remove_props(self):
        cypher = "SET e = {}"

    def update_props(self):
        cypher = "SET e += $map"

    def add_node_label(self, label: str, props:dict):
        """Add a label to a node."""
        where = self.__get_where_part_by_props('n', props)
        cypher = f"""MATCH (n)
            WHERE {where}
            SET n:{label}"""
        self.session.run(cypher)

    def merge_node(self, node:Node):
        """Creates a node with props if not exists"""
        cypher = f"""MERGE (n:{node.label} {node.cypher_props}) return ID(n) as id"""
        try:
            return self.session.run(cypher).data()[0]['id']
        except:
            print(cypher)
            os.system.exit()

    def merge_edge(self, subj:Node, rel:Edge, obj: Node):
        """MERGE finds or creates a relationship between the nodes."""
        cypher = f"""
            MERGE (subject:{subj.label} {subj.cypher_props})
            MERGE (object:{obj.label} {obj.cypher_props})
            MERGE (subject)-[relation:{rel.label} {rel.cypher_props}]->(object)
            RETURN subject, relation, object"""
        return self.session.run(cypher)

    def merge_path(self):
        """MERGE finds or creates paths attached to the node."""
        cypher = """MATCH (a:Person {name: $value1})
            MERGE (a)-[r:KNOWS]->(b:Person {name: $value3})"""

    def delete_edge(self, edge_id: int):
        """Delete an edge by id."""
        cypher = f"""MATCH ()-[r]->()
            WHERE r.id = {edge_id}
            DELETE r"""

    def delete_all_edges(self):
        """Delete all edges."""
        return self.session.run("MATCH ()-[r]->() DELETE r")

    def delete_nodes(self, node: Node):
        """Delete all nodes (and connected edges) with a specific label."""
        constraints_str = node.get_where('n')
        where = f" WHERE {constraints_str}" if constraints_str else ''
        cypher = f"""MATCH (n:{node.label}) {where} DETACH DELETE n """\
            "RETURN count(n) AS number_of_deleted_nodes"
        return self.session.run(cypher).data()[0]['number_of_deleted_nodes']

    def delete_node_and_connected_edges(self, id:int):
        """Delete a node and all relationships/edges connected to it."""
        cypher = f"""MATCH (n)
            WHERE n.id = {id}
            DETACH DELETE n"""
        return self.session.run(cypher)

    def delete_node_edge(self, node_id:int, edge_id:int):
        """Delete a node and a relationship.
        This will throw an error if the node is attached
        to more than one relationship."""
        cypher = f"""MATCH (n)-[r]-()
            WHERE ID(r) = {edge_id} AND ID(n) = {node_id}
            DELETE n, r"""
        return self.session.run(cypher)

    def delete_all(self):
        """Delete all nodes and relationships from the database."""
        return self.session.run("MATCH (n) DETACH DELETE n return count(n) AS num").data()[0]['num']

    def delete_nodes_with_no_edges(self, node: Node):
        cypher_where = ''
        if node.props:
            where = node.get_where('n')
            if where:
                cypher_where = ' AND ' + where
        cypher = f"""MATCH (n: {node.label})
            WHERE NOT (n)-[]-() {cypher_where}
            DELETE n RETURN count(n) AS number_of_deleted_nodes"""
        return self.session.run(cypher).data()[0]['number_of_deleted_nodes']

    def delete_all_nodes_with_no_edges(self):
        cypher = """MATCH (n)
            WHERE NOT (n)-[]-()
            DELETE n RETURN count(n) AS number_of_deleted_nodes"""
        return self.session.run(cypher).data()[0]['number_of_deleted_nodes']

    def get_number_of_nodes(self, node: Optional[Node]=None) -> int:
        where, label = '', ''
        if node:
            where_str = node.get_where('n')
            where = f" WHERE {where_str}" if where_str else ''
            label = f":{node.label}"
        cypher = f"MATCH (n{label}) {where} RETURN count(n) AS num"""
        return self.session.run(cypher).data()[0]['num']

    def get_number_of_edges(self, edge: Optional[Edge]=None) -> int:
        where, label = '', ''
        if edge:
            where_str = edge.get_where('e')
            where = f" WHERE {where_str}" if where_str else ''
            label = f":{edge.label}"
        cypher = f"MATCH ()-[e{label}]-() {where} RETURN count(e) AS num"""
        return self.session.run(cypher).data()[0]['num']

    def delete_edges(self, edge: Edge):
        """Delete all edges."""
        constraints_str = edge.get_where('r')
        where = f" WHERE {constraints_str}" if constraints_str else ''
        cypher = f"MATCH ()-[r:{edge.label}]-() {where} DELETE r """\
            "RETURN count(r) AS n"
        result = self.session.run(cypher)
        if result:
            return result.data()[0]['n']

    def remove_node_label(self, label: str, node_id):
        """Remove a label from a node."""
        cypher = f"""MATCH (n:{label})
            WHERE ID(n) = {node_id}
            REMOVE n:{label}"""
        return self.session.run(cypher)

    def remove_node_prop_by_id(self, node_id:int, prop_name: str):
        """Remove a node property by ID."""
        cypher = f"""MATCH (n)
            WHERE ID(n) = {node_id}
            REMOVE n.{prop_name}"""
        return self.session.run(cypher)

    def remove_node_prop_by_label(self, label:str, prop_name: str):
        """Remove a node property by label."""
        cypher = f"""MATCH (n: {label})
            REMOVE n.{prop_name}"""
        return self.session.run(cypher)

    def remove_all_node_prop_by_id(self, node_id:int, prop_name: str):
        """Remove all node properties by ID."""
        cypher = f"""MATCH (n)
            WHERE ID(n) = {node_id}
            REMOVE n = {{}}"""
        return self.session.run(cypher)

    def remove_all_node_prop_by_label(self, label:str, prop_name: str):
        """Remove a node property by label."""
        cypher = f"""MATCH (n: {label})
            REMOVE n = {{}}"""
        return self.session.run(cypher)

    def list_labels(self):
        """List all labels."""
        return [x['label'] for x in self.exec_data("CALL db.labels() YIELD label")]

    def list_all_columns(self):
        return self.exec_data("CALL db.labels() YIELD *")

    def load_nodes_from_csv(self, label:str, file_path: str, use_cols:Optional[list[str]]=[],
        field_terminator=','):
        """Load nodes from CSV file."""


        import_file = "import_file.csv"
        sym_link = self.import_folder + import_file

        if os.path.exists(sym_link):
            os.remove(sym_link)

        if os.path.exists(file_path):
            os.symlink(file_path, sym_link)

        with open(file_path) as f:
            cols = [x.strip() for x in f.readline().split(field_terminator) if x.strip()]

        if use_cols == []: # if use_cols is an empty list import all available
            use_cols = cols
        elif use_cols: # if use_cols exists, make sure column really exists
            use_cols = [col for col in use_cols if col in cols]
        # if use_cols is None import no props

        import_cols = ""
        if use_cols != None:
            import_cols = "{" + ', '.join([f"{x.lower()}: line.{x}" for x in use_cols]) + "}"

        cypher = f"""LOAD CSV WITH HEADERS FROM
            'file:///{import_file}'
            AS line FIELDTERMINATOR '{field_terminator}'
            CREATE (:{label} {import_cols})"""
        return self.session.run(cypher)

    def __get_chunks(self, list_a, chunk_size=1000):
        for i in range(0, len(list_a), chunk_size):
            yield list_a[i:i + chunk_size]

    def import_nodes_from_mysql(self, label, dict_cursor, sql, database='', merge=False):
        if database:
            dict_cursor.execute(f'use {database}')    
        dict_cursor.execute(sql)
        chunks = self.__get_chunks(dict_cursor.fetchall())
        for rows in tqdm(chunks):
            cyphers = [f"(:{label} {get_cypher_props(row)})" for row in rows]
            if merge:
                for cypher in cyphers:
                    self.session.run(f"MERGE {cypher}")    
            else:
                nodes = ','.join(cyphers)
                cypher = f"CREATE {nodes}"
                self.session.run(cypher)

    def create_node_index(self, index_name: str, label: str, prop_name: str):
        cypher = f"CREATE INDEX {index_name} IF NOT EXISTS FOR (p:{label}) ON (p.{prop_name})"
        return self.session.run(cypher)

    def create_edge_index(self, label: str, prop_name: str, index_name: Optional[str]=None):
        if index_name is None:
            index_name = f"ix_{label}__{prop_name}"
        cypher = f"CREATE INDEX {index_name} IF NOT EXISTS FOR ()-[k:{label}]-() ON (k.{prop_name})"
        return self.session.run(cypher)

    def drop_node_index(self, index_name: str):
        cypher = f"DROP INDEX {index_name} IF EXISTS"
        return self.session.run(cypher)

    def create_unique_constraint(self, label: str, prop_name: str, constraint_name: Optional[str]=None):
        if constraint_name is None:
            constraint_name = f"uid_{label}__{prop_name}"
        cypher = f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop_name} IS UNIQUE"
        return self.session.run(cypher)

    def delete_unique_constraint(self, label, prop_name, constraint_name: Optional[str]=None ):
        if constraint_name is None:
            constraint_name = f"uid_{label}__{prop_name}"
        cypher = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
        return self.session.run(cypher)

    def show_process_list(self):
        cypher = "CALL dbms.listQueries() YIELD queryId, query, database"
        return self.exec_df(cypher)

    def terminate_transactions(self, transaction_ids: Iterable[int]):
        transaction_ids_str = ', '.join([str(x) for x in transaction_ids])
        cypher = "TERMINATE TRANSACTIONS {transaction_ids_str}"
        return self.session.run(cypher)

    def get_node(self, node:Node):
        cypher = f"MATCH (n:{node.label}) where {node.get_where('n')} return n"
        return [x['n'] for x in self.exec_data(cypher)]

"""Main module."""
# import libs and load config
import os
import logging
from neo4j import basic_auth, AsyncGraphDatabase, GraphDatabase
import json
import pandas as pd
from typing import Optional, List, Dict, Iterable, Union, Any

# from sqlalchemy import create_engine
import pandas as pd

# import pymysql
import configparser
from tqdm import tqdm
from collections import namedtuple

# from pymysql import cursors
import numpy as np
import re
from typing_extensions import LiteralString
from neo4j_tools import defaults

Config = namedtuple(
        "Config", ["uri", "user", "password", "import_folder", "database"]
    )

def get_config(config_file_path: str) -> Config:
    file_path = None
    if os.path.exists(config_file_path):
        file_path = config_file_path
    else:
        # shortcut used
        file_path = os.path.join(defaults.PROJECT_DIR,f'config.{config_file_path}.ini')

    config = configparser.ConfigParser()
    config.read(file_path)
    return Config(
        config["NEO4J"]["uri"],
        config["NEO4J"]["user"],
        config["NEO4J"]["password"],
        config["NEO4J"].get("import_folder", None),
        config["NEO4J"].get("database", None),
    )


Relationship = namedtuple("Relationship", ["subj_id", "edge_id", "obj_id"])


def get_standard_name(name: str) -> str:
    """Return standard name."""
    part_of_name = [x for x in re.findall("[A-Z]*[a-z0-9]*", name) if x]
    new_name = "_".join(part_of_name).lower()
    if re.search(r"^\d+", new_name):
        new_name = "_" + new_name
    return new_name


def get_cypher_props(props: Optional[dict]):
    """Convert dictionary to cypher compliant properties as string."""
    props_str = ""
    props_array = []
    if props:
        for k, v in props.items():
            if (isinstance(v, (str, int, list)) and v) or (
                isinstance(v, float) and not np.isnan(v)
            ):
                cypher_str = f"`{k}`: " + json.dumps(v)
                props_array.append(cypher_str)
        if props_array:
            props_str = "{" + ", ".join(props_array) + "}"
    return props_str


# define Node and Edge classes
class GraphElement:
    def __init__(self, labels: Union[str, set[str]], props: Optional[dict] = None):
        if isinstance(labels, str):
            labels = {labels}
        self.labels = labels
        self.props = props

    @property
    def cypher_props(self) -> str:
        return get_cypher_props(self.props)

    @property
    def cypher_labels(self) -> str:
        return ":".join([x.strip() for x in self.labels if x.strip()])

    def __get_sql_value(self, value):
        return json.dumps(value) if isinstance(value, str) else value

    def get_where(self, prefix: str) -> Optional[str]:
        if self.props:
            return " AND ".join(
                [
                    f"{prefix}.{k} = {self.__get_sql_value(v)}"
                    for k, v in self.props.items()
                ]
            )

    def __str__(self):
        return f"<{self.labels}: {self.cypher_props}>"


class Node(GraphElement):
    def __init__(self, labels: Union[str, set[str]], props: Optional[dict] = None):
        super().__init__(labels, props)


class Edge(GraphElement):
    def __init__(self, labels: str, props: Optional[dict] = None):
        super().__init__(labels, props)


py_neo_cast_map = {
    bool: "toBooleanOrNull",
    float: "toFloatOrNull",
    int: "toIntegerOrNull",
}


class Db:
    def __init__(
        self,
        config_file=defaults.config_file_path,
    ):
        print(config_file)
        self.__config = get_config(config_file)
        self.driver = GraphDatabase.driver(
            self.__config.uri, auth=(self.__config.user, self.__config.password), database=self.__config.database
        )
        self.session = self.driver.session()

    def __str__(self):
        return f"<neo4j_tools:Db {{user:{self.__config.user}, database:{self.__config.database}, uri: {self.__config.uri} }}>"

    def exec_data(self, cypher: LiteralString):
        r = self.session.run(cypher)
        return r.data()

    def import_owl(
        self,
        url: str,
        classLabel: str = "Class",
        subClassOfRel: str = "SCO",
        dataTypePropertyLabel: str = "Property",
        objectPropertyLabel: str = "Relationship",
        subPropertyOfRel: str = "SPO",
        domainRel: str = "DOMAIN",
        rangeRel: str = "RANGE",
    ):
        """Import OWL file into Neo4J

        Make sure that you have moved labs/apoc-4.4.0.8-core.jar to plugins/
        https://neo4j.com/labs/apoc/4.4/installation/

        Parameters
        ----------
        url : str
            URL of OWL file
        classLabel : str, optional
            Label to be used for Ontology Classes (categories), by default "Class"
        subClassOfRel : str, optional
            Relationship to be used for rdfs:subClassOf hierarchies, by default "SCO"
        dataTypePropertyLabel : str, optional
            Label to be used for DataType properties in the Ontology, by default "Property"
        objectPropertyLabel : str, optional
            Label to be used for Object properties in the Ontology, by default "Relationship"
        subPropertyOfRel : str, optional
            Relationship to be used for rdfs:subPropertyOf hierarchies, by default "SPO"
        domainRel : str, optional
            Relationship to be used for rdfs:domain, by default "DOMAIN"
        rangeRel : str, optional
            Relationship to be used for rdfs:range, by default "RANGE"
        """
        config = f"""{{
            classLabel: '{classLabel}',
            subClassOfRel: '{subClassOfRel}',
            dataTypePropertyLabel: '{dataTypePropertyLabel}',
            objectPropertyLabel: '{objectPropertyLabel}',
            subPropertyOfRel: '{subPropertyOfRel}',
            domainRel: '{domainRel}',
            rangeRel: '{rangeRel}'
        }}"""
        #self.session.run('CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE')
        self.session.run('call n10s.graphconfig.init()')
        cypher = f'CALL n10s.onto.import.fetch("{url}","Turtle", {config})'
        print(cypher)
        return self.session.run(cypher).data()

    def exec_df(self, cypher: LiteralString):
        data = self.exec_data(cypher)
        if set([len(x.keys()) for x in data]) == {
            1,
        }:
            only_available_key = list(data[0].keys())[0]
            if isinstance(data[0][only_available_key], dict):
                data = [
                    dict(
                        {"Label in Cypher": only_available_key}, **x[only_available_key]
                    )
                    for x in data
                ]
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
        return [x["n"] for x in self.exec_data(cypher)]

    def nodes_by_label(self, labels: Union[set[str], str], limit: Optional[int] = None):
        cypher_limit = f"LIMIT {limit}" if limit else ""
        cypher = f"MATCH (n:{Node(labels).cypher_labels}) RETURN n {cypher_limit}"
        return [x["n"] for x in self.exec_data(cypher)]

    def create_node(self, node: Node) -> int:
        """Create a node with label and properties."""
        cypher = (
            f"CREATE (n:{node.cypher_labels} {node.cypher_props}) return ID(n) as nid"
        )
        return self.session.run(cypher).data()[0]["nid"]

    def __get_sql_value(self, value):
        return f'"' + json.dumps(value) + '"' if isinstance(value, str) else value

    def __get_where_part_by_props(self, node_name: str, props: dict):
        return " AND ".join(
            [f"{node_name}.{k} = {self.__get_sql_value(v)}" for k, v in props.items()]
        )

    def create_edge(self, subj: Node, edge: Edge, obj: Node):
        cypher = f"CREATE (subj:{subj.cypher_labels} {subj.cypher_props})"
        cypher += f"-[edge:{edge.cypher_labels} {edge.cypher_props}]->"
        cypher += f"(obj:{obj.cypher_labels} {obj.cypher_props})" ""
        cypher += " RETURN ID(subj) as subj_id, ID(edge) as edge_id, ID(obj) as obj_id"
        r = self.session.run(cypher).values()[0]
        return Relationship(*r)

    def set_props(self, node_id: int, props: dict):
        # TODO: Implement!
        cypher = """SET
            e.property1 = $value1,
            e.property2 = $value2"""
        return self.exec_data(cypher)[0]

    def remove_props(self):
        cypher = "SET e = {}"

    def update_props(self):
        cypher = "SET e += $map"

    def add_node_label(self, label: str, props: dict):
        """Add a label to a node."""
        where = self.__get_where_part_by_props("n", props)
        cypher = f"""MATCH (n)
            WHERE {where}
            SET n:{label}"""
        self.session.run(cypher)

    def merge_node(self, node: Node):
        """Creates a node with props if not exists"""
        cypher = (
            f"""MERGE (n:{node.cypher_labels} {node.cypher_props}) return ID(n) as id"""
        )
        try:
            return self.session.run(cypher).data()[0]["id"]
        except:
            print(cypher)
            os.system.exit()

    def merge_edge(self, subj: Node, rel: Edge, obj: Node):
        """MERGE finds or creates a relationship between the nodes."""
        cypher = f"""
            MERGE (subject:{subj.cypher_labels} {subj.cypher_props})
            MERGE (object:{obj.cypher_labels} {obj.cypher_props})
            MERGE (subject)-[relation:{rel.cypher_labels} {rel.cypher_props}]->(object)
            RETURN subject, relation, object"""
        return self.session.run(cypher)

    def merge_path(self):
        """MERGE finds or creates paths attached to the node."""
        cypher = """MATCH (a:Person {name: $value1})
            MERGE (a)-[r:KNOWS]->(b:Person {name: $value3})"""

    def delete_edges(self, edge: Edge):
        """Delete edges by Edge class."""
        where = f"WHERE {edge.get_where('r')}" if edge.props else ""
        cypher = f"""MATCH ()-[r:{edge.cypher_labels}]->() {where} DELETE r RETURN count(r) AS num"""
        return self.session.run(cypher).data()[0]["num"]

    def delete_edge_by_id(self, edge_id: int):
        """Delete an edge by id."""
        cypher = f"""MATCH ()-[r]->()
            WHERE r.id = {edge_id}
            DELETE r"""

    def delete_all_edges(self):
        """Delete all edges."""
        return self.session.run("MATCH ()-[r]->() DELETE r")

    def delete_nodes(self, node: Node):
        """Delete all nodes (and connected edges) with a specific label."""
        where = f"WHERE {node.get_where('n')}" if node.props else ""
        cypher = (
            f"""MATCH (n:{node.cypher_labels}) {where} DETACH DELETE n """
            "RETURN count(n) AS num"
        )
        return self.session.run(cypher).data()[0]["num"]

    def delete_node_and_connected_edges(self, id: int):
        """Delete a node and all relationships/edges connected to it."""
        cypher = f"""MATCH (n)
            WHERE n.id = {id}
            DETACH DELETE n"""
        return self.session.run(cypher)

    def delete_node_edge(self, node_id: int, edge_id: int):
        """Delete a node and a relationship.
        This will throw an error if the node is attached
        to more than one relationship."""
        cypher = f"""MATCH (n)-[r]-()
            WHERE ID(r) = {edge_id} AND ID(n) = {node_id}
            DELETE n, r"""
        return self.session.run(cypher)

    def empty_database(self):
        self.recreate_database()

    def recreate_database(self):
        self.session.run(f"DROP DATABASE {self.__config.database}")
        self.session.run(f"CREATE DATABASE {self.__config.database}")

    def delete_all(self) -> int:
        """Delete all nodes and relationships from the database."""
        return self.session.run(
            "MATCH (n) DETACH DELETE n return count(n) AS num"
        ).data()[0]["num"]

    def delete_all_if_many(self, node: Optional[Node] = None):
        """Use this method if many node has to be deleted."""
        if node:
            where = node.get_where("n")
            cypher_where = f" WHERE {where}" if where else ""
            cypher = f"""MATCH (n:{node.cypher_labels}) {cypher_where}
                CALL {{ WITH n
                    DETACH DELETE n
                }} IN TRANSACTIONS OF 10000 ROWS"""
        else:
            cypher = (
                "MATCH (n) CALL { WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS"
            )
        return self.session.run(cypher)

    def delete_nodes_with_no_edges(self, node: Node):
        cypher_where = ""
        if node.props:
            where = node.get_where("n")
            if where:
                cypher_where = " AND " + where
        cypher = f"""MATCH (n: {node.cypher_labels})
            WHERE NOT (n)-[]-() {cypher_where}
            DELETE n RETURN count(n) AS number_of_deleted_nodes"""
        return self.session.run(cypher).data()[0]["number_of_deleted_nodes"]

    def delete_all_nodes_with_no_edges(self):
        cypher = """MATCH (n)
            WHERE NOT (n)-[]-()
            DELETE n RETURN count(n) AS number_of_deleted_nodes"""
        return self.session.run(cypher).data()[0]["number_of_deleted_nodes"]

    def get_number_of_nodes(self, node: Optional[Node] = None) -> int:
        where, label = "", ""
        if node:
            where_str = node.get_where("n")
            where = f" WHERE {where_str}" if where_str else ""
            label = f":{node.cypher_labels}"
        cypher = f"MATCH (n{label}) {where} RETURN count(n) AS num" ""
        return self.session.run(cypher).data()[0]["num"]

    def get_number_of_edges(self, edge: Optional[Edge] = None) -> int:
        where, label = "", ""
        if edge:
            where_str = edge.get_where("e")
            where = f" WHERE {where_str}" if where_str else ""
            label = f":{edge.cypher_labels}"
        cypher = f"MATCH ()-[e{label}]->() {where} RETURN count(e) AS num" ""
        return self.session.run(cypher).data()[0]["num"]

    def remove_node_label(self, labels: Union[set[str], str], node_id):
        """Remove a label(s) from a node."""
        node = Node(labels)
        cypher = f"""MATCH (n:{node.cypher_labels})
            WHERE ID(n) = {node_id}
            REMOVE n:{node.cypher_labels}"""
        return self.session.run(cypher)

    def remove_node_prop_by_id(self, node_id: int, prop_name: str):
        """Remove a node property by ID."""
        cypher = f"""MATCH (n)
            WHERE ID(n) = {node_id}
            REMOVE n.{prop_name}"""
        return self.session.run(cypher)

    def remove_node_prop_by_label(self, labels: Union[set[str], str], prop_name: str):
        """Remove a node property by label."""
        node = Node(labels)
        cypher = f"""MATCH (n: {node.cypher_labels})
            REMOVE n.{prop_name}"""
        return self.session.run(cypher)

    def remove_all_node_prop_by_id(self, node_id: int, prop_name: str):
        """Remove all node properties by ID."""
        cypher = f"""MATCH (n)
            WHERE ID(n) = {node_id}
            REMOVE n = {{}}"""
        return self.session.run(cypher)

    def remove_all_node_prop_by_label(self, label: str, prop_name: str):
        """Remove a node property by label."""
        cypher = f"""MATCH (n: {label})
            REMOVE n = {{}}"""
        return self.session.run(cypher)

    def list_labels(self):
        """List all labels."""
        return [x["label"] for x in self.exec_data("CALL db.labels() YIELD label")]

    def list_all_columns(self):
        return self.exec_data("CALL db.labels() YIELD *")

    def load_nodes_from_csv(
        self,
        label: str,
        file_path: str,
        use_cols: Optional[list[str]] = [],
        field_terminator=",",
    ):
        """Load nodes from CSV file."""

        import_file = "import_file.csv"
        sym_link = self.__config.import_folder + import_file

        if os.path.exists(sym_link):
            os.remove(sym_link)

        if os.path.exists(file_path):
            os.symlink(file_path, sym_link)

        with open(file_path) as f:
            cols = [
                x.strip() for x in f.readline().split(field_terminator) if x.strip()
            ]

        if use_cols == []:  # if use_cols is an empty list import all available
            use_cols = cols
        elif use_cols:  # if use_cols exists, make sure column really exists
            use_cols = [col for col in use_cols if col in cols]
        # if use_cols is None import no props

        import_cols = ""
        if use_cols != None:
            import_cols = (
                "{" + ", ".join([f"{x.lower()}: line.{x}" for x in use_cols]) + "}"
            )

        cypher = f"""LOAD CSV WITH HEADERS FROM
            'file:///{import_file}'
            AS line FIELDTERMINATOR '{field_terminator}'
            CREATE (:{label} {import_cols})"""
        return self.session.run(cypher)

    def __get_chunks(self, list_a, chunk_size=1000):
        for i in range(0, len(list_a), chunk_size):
            yield list_a[i : i + chunk_size]

    def import_nodes_from_mysql(
        self, label, dict_cursor, sql, database="", merge=False
    ):
        if database:
            dict_cursor.execute(f"use {database}")
        dict_cursor.execute(sql)
        chunks = self.__get_chunks(dict_cursor.fetchall())
        for rows in tqdm(chunks):
            cyphers = [f"(:{label} {get_cypher_props(row)})" for row in rows]
            if merge:
                for cypher in cyphers:
                    self.session.run(f"MERGE {cypher}")
            else:
                nodes = ",".join(cyphers)
                cypher = f"CREATE {nodes}"
                self.session.run(cypher)

    def create_node_index(
        self, label: str, prop_name: str, index_name: Optional[str] = ""
    ):
        cypher = f"CREATE INDEX {index_name} IF NOT EXISTS FOR (p:{label}) ON (p.{prop_name})"
        return self.session.run(cypher)

    def create_edge_index(
        self, label: str, prop_name: str, index_name: Optional[str] = None
    ):
        if index_name is None:
            index_name = f"ix_{label}__{prop_name}"
        cypher = f"CREATE INDEX {index_name} IF NOT EXISTS FOR ()-[k:{label}]-() ON (k.{prop_name})"
        return self.session.run(cypher)

    def drop_node_index(self, index_name: str):
        cypher = f"DROP INDEX {index_name} IF EXISTS"
        return self.session.run(cypher)

    def drop_constraint(self, constraint_name):
        cypher = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
        return self.session.run(cypher)

    def create_unique_constraint(
        self, label: str, prop_name: str, constraint_name: Optional[str] = None
    ):
        if constraint_name is None:
            constraint_name = f"uid_{label}__{prop_name}"
        cypher = f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop_name} IS UNIQUE"
        return self.session.run(cypher)

    def delete_unique_constraint(
        self, label, prop_name, constraint_name: Optional[str] = None
    ):
        if constraint_name is None:
            constraint_name = f"uid_{label}__{prop_name}"
        cypher = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
        return self.session.run(cypher)

    def show_process_list(self):
        cypher = "CALL dbms.listQueries() YIELD queryId, query, database"
        return self.exec_df(cypher)

    def terminate_transactions(self, transaction_ids: Iterable[int]):
        transaction_ids_str = ", ".join([str(x) for x in transaction_ids])
        cypher = "TERMINATE TRANSACTIONS {transaction_ids_str}"
        return self.session.run(cypher)

    def get_node(self, node: Node):
        cypher = f"MATCH (n:{node.cypher_labels}) where {node.get_where('n')} return n"
        return [x["n"] for x in self.exec_data(cypher)]

    def get_node_by_id(self, node_id: int):
        cypher = f"MATCH (n) WHERE ID(n) = {node_id} RETURN n LIMIT 1"
        values = self.session.run(cypher).values()
        if values:
            return values[0][0]

    def get_edge_by_id(self, edge_id: int):
        cypher = f"MATCH (n)-[r]->(m) WHERE ID(r) = {edge_id} RETURN n,r,m LIMIT 1"
        values = self.session.run(cypher).values()
        if values:
            return values[0]

    def show_procedures(self):
        return self.session.run("CALL dbms.procedures()").to_df()

    def get_edge_types_by_prefix(self, prefix: str):
        cypher = (
            f"MATCH ()-[r]->() where type(r)=~'^{prefix}__.*' RETURN distinct type(r)"
        )
        return [x[0] for x in self.session.run(cypher).values()]

    def count_nodes(self, node: Node):
        cypher = f"match (n:{node.cypher_labels}) return count(n) as num"
        return self.session.run(cypher).data()[0]["num"]

    def count_edges(self, edge: Edge):
        cypher = f"match ()-[r:{edge.cypher_labels}]->() return count(r) as num"
        return self.session.run(cypher).data()[0]["num"]

    def exec_large_cypher(
        self, cypher: Union[str, list[str]], cypher_file_path="temp_cypher.txt"
    ):
        if isinstance(cypher, list):
            cypher = " ".join(cypher) + ";"

        with open(cypher_file_path, "w") as cypher_file:
            cypher_file.write(cypher)

        if os.path.exists(cypher_file_path):
            address = "bolt+s://" + self.__config.uri.split("://")[-1]
            command = f"cat {cypher_file_path} | cypher-shell -u {self.__config.user} -p {self.__config.password} -a {address} -d {self.__config.database} --format plain"
            #print(command)
            output = os.popen(command).read()
            os.remove(cypher_file_path)
            return command

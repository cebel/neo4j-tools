# Neo4J Tools

## Install 

```bash
pip install git+https://github.com/cebel/neo4j-tools.git
```

Before using the lib create a config file with

localhost:

```bash
neo4j_tools set-neo4j-config -u user -p password -d database -s server_name -o port -i import_folder
```

## Usage

localhost

```python
from neo4j_tools.neo4j_tools import Db
db = Db()
```

any other server

```python
from neo4j_tools.neo4j_tools import Db
db = Db(uri="bolt://ip_or_server_name:7687",password='your_password', user='user_name')
```



TODO: Need more explanation here

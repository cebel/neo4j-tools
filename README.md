# Neo4J Tools


create a config file `~/.neo4j-tools/config.ini`

with

```bash
[MYSQL]
user = mysql_user
password = mysql_password
database = database_name
host = localhost

[NEO4J]
uri = bolt://localhost:7687
user = neo4j
password = neo4j_password
import_folder = /opt/neo4j/import
```

## Usage

```python
from neo4j_tools.neo4j_tools import Db
db = Db()
```

## Tools

### Examples

* db.show_process_list()

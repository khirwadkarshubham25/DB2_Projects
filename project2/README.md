# MONGO DB

-----------------------------------------------------
### Version
Python ~ 3.8

-----------------------------------------------------
### Execution

1. For all code execution please execute code from inside the source directory DB2-Project2

2. To create mongo db and collections execute following command in console

    
    python ./scripts/mongo_db_creation.py mongodb://localhost:5000


    python ./scripts/mongo_db_creation.py {localhost url}


3. To retrieve data from mongo db schema execute following query in console. Query result will be stored in output files directory.


    python ./scripts/get_db_data.py mongodb://localhost:5000

    python ./scripts/get_db_data.py {localhost url}

4. If url is not provided then the code will use cloud mongo db server for schema.

-----------------------------------------------------------

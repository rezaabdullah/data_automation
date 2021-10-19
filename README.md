# Data Automation Boilerplate Script
Boilerplate script for connecting MySQL Database

### .env
Environment file is in the directory to encapsulate sensitive information related to database.
```
USER=<database_user_name>
PASSWORD=<database_password>
DATABASE_NAME=<database_name>
SERVER=<server_ip_address or localhost>
PORT=<port number usually 3306>
```

### Jupyter or Python Script
```python
# Engine configuration
cnx_str = "mysql+pymysql://{user}:{pw}@{server}:{port}/{db}".format(user=USER, pw=PASSWORD, server=SERVER, 
                port=PORT, db=DATABASE)
cnx = create_engine(cnx_str).connect()
```

`cnx = create_engine(cnx_str).connect()` this line will eliminate `OperationalError` that was generated when the table is created at the beginning

For safety also close the connection at the end `cnx.close()`

### Instructions
1. run `python setup.py install` to install all the dependencies and modules
2. Alternatively run `pip install -r requirements.txt`

Rest of the code can be incorporated within the script
import ConfigParser,vertica_python

cf = ConfigParser.ConfigParser()
cf.read("conf.ini")
ghe_url = cf.get("github", "ghe_url")
token = cf.get("github", "token")
# get database config
db_host = cf.get("vertica_db", "db_host")
db_port = cf.getint("vertica_db", "db_port")
db_user = cf.get("vertica_db", "db_user")
db_pass = cf.get("vertica_db", "db_pass")
db_database = cf.get("vertica_db", "db_database")
read_timeout = cf.getint("vertica_db", "read_timeout")

def vertica_BuildConnect():
    """create a vertica conntion object"""
    conn_info = {'host': db_host,
                 'port': db_port,
                 'user': db_user,
                 'password': db_pass,
                 'database': db_database,
                 # 10 minutes timeout on queries
                 'read_timeout': read_timeout
                 }
    connection = vertica_python.connect(**conn_info)
    # print(connection)
    return connection
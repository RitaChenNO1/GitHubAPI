__author__ = 'chzhenzh'
import urllib2,json,vertica_python,ConfigParser,nested_dict
from nested_dict import nested_dict
#get github config
cf = ConfigParser.ConfigParser()
cf.read("conf.ini")
ghe_url = cf.get("github", "ghe_url")
token = cf.get("github", "token")
#get database config
db_host = cf.get("vertica_db", "db_host")
db_port = cf.getint("vertica_db", "db_port")
db_user = cf.get("vertica_db", "db_user")
db_pass = cf.get("vertica_db", "db_pass")
db_database = cf.get("vertica_db", "db_database")
read_timeout = cf.getint("vertica_db", "read_timeout")

#get the last since value
def get_preSince(filename,apistr):
    usercf = ConfigParser.SafeConfigParser()
    usercf.read(filename)
    pre_since = usercf.getint(apistr, "pre_since")
    return pre_since

def set_preSince(filename,apistr,since_last):
    usercf = ConfigParser.SafeConfigParser()
    usercf.read(filename)
    usercf.set(apistr,"pre_since",since_last)
    usercf.write(open(filename, "w"))


#connect to vertica
def vertica_BuildConnect():
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
#the tableName to receive the data, the input jsonData, the vertica cur
def json2VerticaTable(tableName,jsonData,cur):
        i=0
        for r in jsonData:
                #since >=2 level, the 2nd level's data will be a problem
                columns = []
                values = []
                nd = nested_dict(r)
                for keys_as_tuple, value in nd.items_flat():
                    columns.append("_".join(str(x) for x in keys_as_tuple))
                    #since the ' is inside the value, but ' should nt in a insert SQL
                    values.append(str(value).replace("'","''"))
                if i == 0:
                        #CREATE TABLE foo (numbs int, names varchar(30))
                        createTableSQL="CREATE TABLE IF NOT EXISTS  "+tableName+" ("
                        for k in columns:
                                createTableSQL=createTableSQL+k+" varchar(200),"
                        #remove the last ,  and add 0
                        createTableSQL=createTableSQL[:-1]+")"
                        #print(createTableSQL)
                        cur.execute(createTableSQL)
                        i=i+1
                else:
                        columnsStr=','.join(columns)
                        #print(columnsStr)
                        #TypeError: sequence item 6: expected string or Unicode, bool found
                        valuesStr=','.join(["'"+str(x)+"'" for x in values])
                        #print(valuesStr)
                        insertSQL="INSERT INTO %s (%s) VALUES (%s)" %(tableName,columnsStr,valuesStr)
                        #print(insertSQL)
                        cur.execute(insertSQL)
                        i=i+1

def ghe_get_nextBatch(header_link):
        if header_link is None:
            return "", "0"
        else:
            next_found = header_link.find("since=")
            print type(next_found)
            print next_found
            if(0 < next_found):
                    start = header_link.find("https:")
                    end = header_link.find(">;")
            else:
                    return "", "0"
        return header_link[start:end], header_link[next_found+len("since="):end]


def ghe_getList(tableName,type,pre_since,per_page,filename,apistr):
        #1. set the url, and start batch
        #every time, start from the since value of last batch, incremental to add data
        # https://github.hpe.com/api/v3/users/per_page=100&since=305
        url = ghe_url+"/api/v3/%s?per_page=%s&since=%s" %(type,per_page,pre_since)
        print(url)

        next_batch_exists=True
        #2. create vertica connection
        vertica_con=vertica_BuildConnect()
        cur = vertica_con.cursor()
        since_last=pre_since
        while next_batch_exists:
            #3. start to request data for every batch, every batch 30 rows by default
            request = urllib2.Request(url)
            request.add_header('Authorization', 'token %s' % token)
            response = urllib2.urlopen(request, timeout = 1000)
            content = response.read()
            print(response.info())
            #print(content)
            repos_batch = json.loads(content)
            #4. insert data to vertica
            json2VerticaTable(tableName,repos_batch,cur)
            vertica_con.commit()
            #5. Get the url for the next batch
            url_next, since_next = ghe_get_nextBatch(response.info().getheader('Link'))
            print("since=:"+since_next)
            #check next bactch is there or not
            if(len(url_next)!=0):
                url = url_next
                since_last=since_next
                set_preSince(filename,apistr,since_last)
            else:
                next_batch_exists = False
                #remember the since id to config
        #6. close the connection of vertica
        vertica_con.close()
        print ("***************************One Batch Loading Done****************************************")

# pre_since = get_preSince("user.ini","user")
# ghe_getList("gitlist.userslist","users",pre_since,"100","user.ini","user")
pre_since = get_preSince("org.ini","org")
ghe_getList("gitlist.orgList","organizations",pre_since,"100","org.ini","org")

# pre_since = get_preSince("team.ini","team")
# ghe_getList("gitlist.teamList","orgs/FutureState/teams",pre_since,"100","team.ini","team")
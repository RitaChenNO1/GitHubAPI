import urllib2,json,vertica_python,ConfigParser
import global_var as gl
from nested_dict import nested_dict


# 1. read from vertica table orgList {id, login} id is the PK, named A
# 2. read from vertica table readOrgs , if not exist, create this table first, named B
# 3. get the subset of C=A-B
# 4. get orgs/:org/teams(:org is from C iteratively) and insert into vertica table teamList

# define functions
vertica_con=gl.vertica_BuildConnect()
cur = vertica_con.cursor()
orgListSQL="select distinct id, login from gitlist.orgList"
readOrgSQL="select distinct org_id, org_name from gitlist.readOrg"
tableA=["orgList"]
tableB=["readOrg"]
keep_keys={'id','name','repositories_url'}
extend_cols=['org_id','org_name']
extend_vals=[]
tableName='gitlist.test'

def get_orgs(cur):
    """for teams, get the next Batch orgs {login,id} """
    cur.execute("select table_name from all_tables where schema_name='gitlist';")
    allTable = cur.fetchall()
    if tableA in allTable:
        cur.execute(orgListSQL)
        orgList = cur.fetchall()
        if tableB not in allTable:
            cur.execute("create table gitlist."+tableB[-1]+"(org_id varchar(200),org_name varchar(200))")
        cur.execute(readOrgSQL)
        readOrg = cur.fetchall()
        vertica_con.commit()
    else:
        print("***********there are no organizaiton data currently******************")
    subset = [i for i in orgList if i not in readOrg]
    return subset


def json2VerticaTable(tableName, jsonData, cur, keep_keys,extend_cols,extend_vals):
    """insert choosed {key,value} of data into Vertica for each Batch"""
    i = 0
    for r in jsonData:
        # since >=2 level, the 2nd level's data will be a problem
        columns = []
        values = []
        nd = nested_dict(r)
        for keys_as_tuple, value in nd.items_flat():
            con_keys = "_".join(str(x) for x in keys_as_tuple)
            if con_keys in keep_keys:
                columns.append(con_keys)
                values.append(str(value).replace("'", "''"))
        columns.extend(extend_cols)
        values.extend(extend_vals)
        # print("**************this is the columns******************")
        # print columns
        # print("**************this is the values******************")
        # print values
        if i == 0:
            # CREATE TABLE foo (numbs int, names varchar(30))
            createTableSQL = "CREATE TABLE IF NOT EXISTS  " + tableName + " ("
            for k in columns:
                createTableSQL = createTableSQL + k + " varchar(200),"
            # remove the last ,  and add 0
            createTableSQL = createTableSQL[:-1] + ")"
            # print(createTableSQL)
            cur.execute(createTableSQL)
            i = i + 1
        else:
            columnsStr = ','.join(columns)
            # print(columnsStr)
            # TypeError: sequence item 6: expected string or Unicode, bool found
            valuesStr = ','.join(["'" + str(x) + "'" for x in values])
            # print(valuesStr)
            insertSQL = "INSERT INTO %s (%s) VALUES (%s)" % (tableName, columnsStr, valuesStr)
            # print(insertSQL)
            cur.execute(insertSQL)
            i = i + 1


def ghe_get_nextBatch(header_link,pageno):
    if header_link is None:
        return 0
    elif "rel=\"next\"" in header_link:
        return pageno+1
    else:
        return 0

def step_two():
    for e in orgs:
        print("**********************one org**********************")
        print(e)
        pageno = 1
        next_batch_exists = True

        while next_batch_exists:
            print("****************pageno =*********" + str(pageno))
            url = gl.ghe_url + "/api/v3/%s?page=%s" % ("orgs/" + e[1] + "/teams", pageno)
            print ("this is the url : " + url)
            request = urllib2.Request(url)
            request.add_header('Authorization', 'token %s' % gl.token)
            response = urllib2.urlopen(request, timeout=1000)
            content = response.read()
            print(response.info())
            next_batch_exists = True
            repos_batch = json.loads(content)
            print("the lenght of this json data is : %d" % (len(repos_batch)))
            json2VerticaTable(tableName, repos_batch, cur, keep_keys, extend_cols, e)
            vertica_con.commit()
            pageno = ghe_get_nextBatch(response.info().getheader('Link'), pageno)
            if (pageno == 0):
                next_batch_exists = False
                print("*******************for this org, data has been processed completely****************************")
        # after write these orgs' team info to Vertica successfully, keep these orgs into Vertica too
        columnsStr = ','.join(str(x) for x in extend_cols)
        valuesStr = ','.join(["'" + str(x) + "'" for x in e])
        cur.execute("INSERT INTO %s (%s) VALUES (%s)" % ('gitlist.' + tableB[-1], columnsStr, valuesStr))
        vertica_con.commit()


# step one => get un-read orgs
orgs = get_orgs(cur)
print("****the number of orgs is : %d" %(len(orgs)))
# orgs=[['670','HPE-Marketing']]
step_two()
vertica_con.close()




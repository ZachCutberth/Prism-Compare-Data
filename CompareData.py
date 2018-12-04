# Compare inventory Price/Qty between POA and store servers.
# By Zach Cutberth

import cx_Oracle
import pymysql
import config
import os

def query_oracle_store_info(store_hostname):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_sbs_no = """select sbs_no from rps.subsidiary
                    where sid in (select default_sbs_sid from rps.controller where address = '""" + store_hostname + """')"""

    get_store_no = """select store_no from rps.store
                      where sid in (select store_sid from rps.controller where address = '""" + store_hostname + """')"""

    cursor.execute(get_sbs_no)
    sbs_no = cursor.fetchone()
    sbs_no = sbs_no[0]
    # print(sbs_no)
    cursor.execute(get_store_no)
    store_no = cursor.fetchone()
    store_no = store_no[0]
    # print(store_no)
    cursor.close()
    dbconnection.close()

    return {'sbs_no':sbs_no, 'store_no':store_no}

def query_oracle_total_qty(store_hostname, sbs_no, store_no):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_total_qty = """select sum(qty) from cms.invn_sbs_qty 
                       where sbs_no = """ + str(sbs_no) + """ and store_no = """ + str(store_no) + """ 
                       and item_sid in (select item_sid from cms.invn_sbs where active = 1)"""

    cursor.execute(get_total_qty)
    oracle_total_qty = cursor.fetchone()
    oracle_total_qty = oracle_total_qty[0]

    cursor.close()
    dbconnection.close()

    return oracle_total_qty

def query_oracle_invn_list(store_hostname, sbs_no, store_no):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_invn_list = """select item_sid, qty from cms.invn_sbs_qty 
                       where sbs_no = """ + str(sbs_no) + """ and store_no = """ + str(store_no) + """ 
                       and item_sid in (select item_sid from cms.invn_sbs where active = 1)
                       and qty is not null
                       order by item_sid"""
    
    print('Getting POA list of inventory...')
    cursor.execute(get_invn_list)
    oracle_invn_list = cursor.fetchall()

    cursor.close()
    dbconnection.close()

    return oracle_invn_list


def query_mysql_total_qty(store_hostname, sbs_no, store_no):
    # Open database connection
    db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    cursor.execute("""select sum(qty) from rpsods.invn_sbs_item_qty 
                      where invn_sbs_item_sid in (select sid from rpsods.invn_sbs_item where active = 1) 
                      and sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """) 
                      and store_sid in (select sid from rpsods.store where store_no = """ + str(store_no) + """);""")

    # Fetch a single row using fetchone() method.
    mysql_total_qty = cursor.fetchone()
    mysql_total_qty = int(mysql_total_qty[0])
    

    # disconnect from server
    db.close()
    
    return mysql_total_qty
    
def query_mysql_invn_list(store_hostname, sbs_no, store_no):
    # Open database connection
    db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')

    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    print('Getting ' + store_hostname + ' list of inventory...')
    # execute SQL query using execute() method.
    cursor.execute("""select b.invn_item_uid, a.qty 
                      from rpsods.invn_sbs_item_qty as a 
                      left join rpsods.invn_sbs_item as b on a.invn_sbs_item_sid = b.sid
                      where a.invn_sbs_item_sid in (select sid from rpsods.invn_sbs_item where active = 1) 
                      and a.sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """) 
                      and a.store_sid in (select sid from rpsods.store where store_no = """ + str(store_no) + """)
                      and a.qty is not null
                      group by b.invn_item_uid;""")

    # Fetch a single row using fetchone() method.
    mysql_invn_list = cursor.fetchall()
    #mysql_invn_list = (mysql_total_qty[0])
    

    # disconnect from server
    db.close()
    
    return mysql_invn_list

def compare_total_qty(store_hostname, sbs_no, store_no):
    oracle_total_qty = query_oracle_total_qty(store_hostname, sbs_no, store_no)
    mysql_total_qty = query_mysql_total_qty(store_hostname, sbs_no, store_no)

    if oracle_total_qty == mysql_total_qty:
        print('POA Qty: ' + str(oracle_total_qty))
        print(store_hostname + ' Qty:' + str(mysql_total_qty))
        print('Equal.')
        file.write('POA Qty: ' + str(oracle_total_qty) + '\n')
        file.write(store_hostname + ' Qty:' + str(mysql_total_qty) + '\n')
        file.write('Equal.')
        return('Equal')

    if oracle_total_qty != mysql_total_qty:
        print('POA Qty: ' + str(oracle_total_qty))
        print(store_hostname + ' Qty: ' + str(mysql_total_qty))
        print('Not Equal.')
        file.write('POA Qty: ' + str(oracle_total_qty) + '\n')
        file.write(store_hostname + ' Qty: ' + str(mysql_total_qty) + '\n')
        file.write('Not Equal.\n')
        return('Not Equal')

def compare_lists(store_hostname, sbs_no, store_no, oracle_invn_list, mysql_invn_list):
    print('Comparing quantities between POA and store ' + store_hostname + '...')
    for key in oracle_invn_list:
        if not key in mysql_invn_list:
            print('Item SID: ' + str(key) + ' not found at ' + store_hostname + '.')
            file.write('Item SID: ' + str(key) + ' not found at ' + store_hostname + '.\n')
            oracle_rep_check(sbs_no, store_no, key)
            continue
        if oracle_invn_list[key] != mysql_invn_list[key]:
            print('Item SID: ' + str(key) + ' quantity not equal. POA Qty: ' + str(oracle_invn_list[key]) + ' ' + store_hostname + ' Qty: ' + str(mysql_invn_list[key]))
            file.write('Item SID: ' + str(key) + ' quantity not equal. POA Qty: ' + str(oracle_invn_list[key]) + ' ' + store_hostname + ' Qty: ' + str(mysql_invn_list[key]) + '\n')
            rep_check = oracle_rep_check(sbs_no, store_no, key)
            if rep_check != True:
                mysql_rep_check(sbs_no, store_no, key)
    for key in mysql_invn_list:
        if not key in oracle_invn_list:                
            print('Item SID: ' + str(key) + ' not found at POA.')
            file.write('Item SID: ' + str(key) + ' not found at POA.\n')

def mysql_rep_check(sbs_no, store_no, key):
    # Open database connection
    db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')

    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    # execute SQL query using execute() method.
    cursor.execute("""select count(*) from rpsods.pub_dataevent_queue where resource_sid in (select sid from rpsods.invn_sbs_item where invn_item_uid = """ + str(key) + """);""")

    # Fetch a single row using fetchone() method.
    mysql_rep_check = cursor.fetchone()
    #mysql_invn_list = (mysql_total_qty[0])
    

    # disconnect from server
    db.close()
    
    if int(mysql_rep_check[0]) > 0:
        print('Item SID: ' + str(key) + ' is waiting to be replicated from the store to the POA.')
        file.write('Item SID: ' + str(key) + ' is waiting to be replicated from the store to the POA.\n')
        return

def oracle_rep_check(sbs_no, store_no, key):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    drs_rep_check = """select count(*) from drs.drs_invn_sbs_qty 
                    where item_sid = """ + str(key) + """ and sbs_no = """ + str(sbs_no) + """ and store_no = """ + str(store_no) + """""" 
        
    
    cursor.execute(drs_rep_check)
    oracle_drs_rep_check = cursor.fetchone()

    pub_rep_check = """select count(*) from drs.pub_invn_sbs_qty 
                    where item_sid = """ + str(key) + """ and sbs_no = """ + str(sbs_no) + """ and store_no = """ + str(store_no) + """""" 
    
    cursor.execute(pub_rep_check)
    oracle_pub_rep_check = cursor.fetchone()

    cursor.close()
    dbconnection.close()

    if int(oracle_drs_rep_check[0]) > 0:
        print('Item SID: ' + str(key) + ' is waiting to be replicated from the POA to the store.')
        file.write('Item SID: ' + str(key) + ' is waiting to be replicated from the POA to the store.\n')
        return True
    if int(oracle_pub_rep_check[0]) > 0:
        print('Item SID: ' + str(key) + ' is waiting to be replicated from the POA to the store.')
        file.write('Item SID: ' + str(key) + ' is waiting to be replicated from the POA to the store.\n')
        return True
            
store_hostname = input('Enter the store servers hostname: ')
try:
    os.remove(store_hostname + '_results.txt')
except OSError:
    pass
file = open(store_hostname + '_results.txt', 'w')
print(store_hostname)
store_info = query_oracle_store_info(store_hostname)

total_qty = compare_total_qty(store_hostname, store_info['sbs_no'], store_info['store_no'])

if total_qty == 'Not Equal':
    oracle_invn_list = query_oracle_invn_list(store_hostname, store_info['sbs_no'], store_info['store_no'])
    oracle_invn_list = dict(oracle_invn_list)
    oracle_invn_list = {key:int(value) for key, value in oracle_invn_list.items()}

    mysql_invn_list = query_mysql_invn_list(store_hostname, store_info['sbs_no'], store_info['store_no'])
    mysql_invn_list = dict(mysql_invn_list)
    mysql_invn_list = {key:int(value) for key, value in mysql_invn_list.items()}
    
    compare_lists(store_hostname, store_info['sbs_no'], store_info['store_no'], oracle_invn_list, mysql_invn_list)

file.close()
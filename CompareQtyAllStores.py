# Compare inventory Price/Qty between POA and store servers.
# By Zach Cutberth

import cx_Oracle
import pymysql
import config
import os
import time
import csv

def check_host_is_correct(store_hostname):
    # Open database connection
    try:
        db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')
    except:
        print('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database.')
        file.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n')
        summary.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        file.close()
        return 'Offline'
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    cursor.execute("""select address from rpsods.controller
                      where active = 1;""")

    # Fetch a single row using fetchone() method.
    address = cursor.fetchone()
    address = str(address[0])
    

    # disconnect from server
    db.close()
    
    if address == store_hostname:
        print('Store Hostname: ' + store_hostname + ' matches with active controller record address: ' + address)
        file.write('Store Hostname: ' + store_hostname + ' matches with active controller record address: ' + address + '\n')
        summary.write('Store Hostname: ' + store_hostname + ' matches with active controller record address: ' + address + '\n')
        return True
    else:
        print('Store Hostname: ' + store_hostname + ' does not match with active controller record address: ' + address)
        file.write('Store Hostname: ' + store_hostname + ' does not match with active controller record address: ' + address + '\n')
        summary.write('Store Hostname: ' + store_hostname + ' does not match with active controller record address: ' + address + '\n\n')
        summary.close()
        return False

def query_oracle_store_info(store_code):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_sbs_no = """select sbs_no from rps.subsidiary
                    where sid in (select sbs_sid from rps.store where store_code = '""" + store_code + """')"""

    get_store_no = """select store_no from rps.store
                      where store_code = '""" + store_code + """'"""

    cursor.execute(get_sbs_no)
    sbs_no = cursor.fetchone()
    print(sbs_no)
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
                       and item_sid in (select item_sid from cms.invn_sbs where active = 1 and sbs_no = """ + str(sbs_no) + """)"""

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
                       and item_sid in (select item_sid from cms.invn_sbs where active = 1 and sbs_no = """ + str(sbs_no) + """)
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
    try:
        db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')
    except:
        print('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database.')
        file.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n')
        errors.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        summary.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        file.close()
        summary.close()
        errors.close()
        return 'Offline'
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    cursor.execute("""select sum(qty) from rpsods.invn_sbs_item_qty 
                      where invn_sbs_item_sid in (select sid from rpsods.invn_sbs_item where active = 1 and sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """)) 
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
    try:
        db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')
    except:
        print('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database.')
        file.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n')
        errors.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        errors.close()
        file.close()
        return 'Offline'
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    print('Getting ' + store_hostname + ' list of inventory...')
    # execute SQL query using execute() method.
    cursor.execute("""select b.invn_item_uid, a.qty 
                      from rpsods.invn_sbs_item_qty as a 
                      left join rpsods.invn_sbs_item as b on a.invn_sbs_item_sid = b.sid
                      where a.invn_sbs_item_sid in (select sid from rpsods.invn_sbs_item where active = 1 and sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """)) 
                      and a.sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """) 
                      and a.store_sid in (select sid from rpsods.store where store_no = """ + str(store_no) + """)
                      and a.qty is not null
                      order by b.invn_item_uid;""")

    # Fetch a single row using fetchone() method.
    mysql_invn_list = cursor.fetchall()
    #mysql_invn_list = (mysql_total_qty[0])
    

    # disconnect from server
    db.close()
    
    return mysql_invn_list

def compare_total_qty(store_hostname, sbs_no, store_no):
    oracle_total_qty = query_oracle_total_qty(store_hostname, sbs_no, store_no)
    mysql_total_qty = query_mysql_total_qty(store_hostname, sbs_no, store_no)

    if mysql_total_qty == 'Offline':
        return 'Offline'

    if oracle_total_qty == mysql_total_qty:
        print('POA Qty: ' + str(oracle_total_qty))
        print(store_hostname + ' Qty:' + str(mysql_total_qty))
        print('Equal.')
        file.write('POA Qty: ' + str(oracle_total_qty) + '\n')
        summary.write('POA Qty: ' + str(oracle_total_qty) + '\n')
        file.write(store_hostname + ' Qty: ' + str(mysql_total_qty) + '\n')
        summary.write(store_hostname + ' Qty: ' + str(mysql_total_qty) + '\n')
        file.write('Equal. \n')
        summary.write('Equal. \n\n')
        return('Equal')

    if oracle_total_qty != mysql_total_qty:
        print('POA Qty: ' + str(oracle_total_qty))
        print(store_hostname + ' Qty: ' + str(mysql_total_qty))
        print('Not Equal.')
        file.write('POA Qty: ' + str(oracle_total_qty) + '\n')
        summary.write('POA Qty: ' + str(oracle_total_qty) + '\n')
        file.write(store_hostname + ' Qty: ' + str(mysql_total_qty) + '\n')
        summary.write(store_hostname + ' Qty: ' + str(mysql_total_qty) + '\n')
        file.write('Not Equal.\n')
        summary.write('Not Equal. \n\n')
        return('Not Equal')

def compare_lists(store_hostname, sbs_no, store_no, oracle_invn_list, mysql_invn_list, resend_data):
    print('Comparing quantities between POA and store ' + store_hostname + '...')
    for key in oracle_invn_list:
        if not key in mysql_invn_list:
            if str(oracle_invn_list[key]) != '0':
                print('Item SID: ' + str(key) + ' not found at ' + store_hostname + '.')
                file.write('Item SID: ' + str(key) + ' not found at ' + store_hostname + '.\n')
                rep_check_oracle = oracle_rep_check(sbs_no, store_no, key)
                print('rep_check_oracle = ' + str(rep_check_oracle))
                file.write('rep_check_oracle = ' + str(rep_check_oracle) + '\n')
                if rep_check_oracle != True:
                    if resend_data == True:
                        print('Resending data...')
                        file.write('Resending data...' + '\n')
                        resend_item_v9(key, sbs_no, store_no)
                continue
        if oracle_invn_list[key] != mysql_invn_list[key]:
            print('Item SID: ' + str(key) + ' quantity not equal. POA Qty: ' + str(oracle_invn_list[key]) + ' ' + store_hostname + ' Qty: ' + str(mysql_invn_list[key]))
            file.write('Item SID: ' + str(key) + ' quantity not equal. POA Qty: ' + str(oracle_invn_list[key]) + ' ' + store_hostname + ' Qty: ' + str(mysql_invn_list[key]) + '\n')
            rep_check_oracle = oracle_rep_check(sbs_no, store_no, key)
            print('rep_check_oracle = ' + str(rep_check_oracle))
            file.write('rep_check_oracle = ' + str(rep_check_oracle) + '\n')
            if rep_check_oracle != True:
                rep_check_mysql = mysql_rep_check(sbs_no, store_no, key)
                print('rep_check_mysql = ' + str(rep_check_mysql))
                file.write('rep_check_mysql = ' + str(rep_check_mysql) + '\n')
                if rep_check_mysql == 'Offline':
                    return 'Offline'
                if rep_check_mysql == False and resend_data == True:
                    print('Resending data...')
                    file.write('Resending data...' + '\n')
                    resend_item_v9(key, sbs_no, store_no)

    for key in mysql_invn_list:
        if not key in oracle_invn_list:                
            print('Item SID: ' + str(key) + ' not found at POA.')
            file.write('Item SID: ' + str(key) + ' not found at POA.\n')

    file.flush()
    os.fsync(file.fileno())
    summary.flush()
    os.fsync(summary.fileno())

def mysql_rep_check(sbs_no, store_no, key):
    # Open database connection
    try:
        db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')
    except:
        print('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database.')
        file.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n')
        errors.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        summary.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        summary.close()
        file.close()
        errors.close()
        return 'Offline'
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
        return True
    else:
        return False

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

def resend_item_v9(item_sid, sbs_no, store_no):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    resend_invn = """update cms.invn_sbs set modified_date = modified_date where item_sid = """ + str(item_sid) + """ 
                     and sbs_no = """ + str(sbs_no) + """"""

    resend_invn_qty = """update cms.invn_sbs_qty set qty = qty where item_sid = """ + str(item_sid) + """ 
                      and sbs_no = """ + str(sbs_no) + """
                      and store_no = """ + str(store_no) + """"""

    resend_invn_price = """update cms.invn_sbs_price set sbs_no = sbs_no where item_sid = """ + str(item_sid) + """ 
                        and sbs_no = """ + str(sbs_no) + """"""

    commit = """commit"""

    cursor.execute(resend_invn)
    cursor.execute(resend_invn_qty)
    cursor.execute(resend_invn_price)
    cursor.execute(commit)

    print('Item SID: ' + str(item_sid) + ' has been queued for replication from the POA to the store.')
    file.write('Item SID: ' + str(item_sid) + ' has been queued for replication from the POA to the store.\n')

    cursor.close()
    dbconnection.close()

time_and_date = time.strftime("%Y-%m-%d_%H-%M-%S")
#store_hostname = input('Enter the store servers hostname: ')
#store_code = input('Enter the store code: ')
#resend_data = input("Replicate items where quantites don't match? y/n: ")

with open('stores.txt') as f:
    store_list = csv.reader(f, skipinitialspace=True)

    for store in store_list:
        store_hostname = store[0]
        store_code = store[1]

        resend_data = 'y'

        if resend_data == 'y':
            resend_data = True
        else: 
            resend_data = False

        if not os.path.exists('Qty_' + str(time_and_date)):
            os.makedirs('Qty_' + str(time_and_date))

        summary = open('Qty_' + str(time_and_date) + '\\Qty_Summary_' + time_and_date + '.txt', 'a')
        errors = open('Qty_' + str(time_and_date) + '\\Qty_Errors_' + time_and_date + '.txt', 'a')
        file = open('Qty_' + str(time_and_date) + '\\Qty_' + store_hostname + '_' + time_and_date + '.txt', 'w')
        
        print('Store Hostname: ' + store_hostname)
        file.write('Store Hostname: ' + store_hostname + '\n')
        summary.write('Store Hostname: ' + store_hostname + '\n')
        print('Store Code: ' + store_code)
        file.write('Store Code: ' + store_code + '\n')
        summary.write('Store Code: ' + store_code + '\n')
        store_info = query_oracle_store_info(store_code)
        print('SBS No: ' + str(store_info['sbs_no']))
        file.write('SBS No: ' + str(store_info['sbs_no']) + '\n')
        summary.write('SBS No: ' + str(store_info['sbs_no']) + '\n')
        print('Store No: ' + str(store_info['store_no']))
        file.write('Store No: ' + str(store_info['store_no']) + '\n')
        summary.write('Store No: ' + str(store_info['store_no']) + '\n')
        print('Resend Data: ' + str(resend_data))
        file.write('Resend Data: ' + str(resend_data) + '\n')
        summary.write('Resend Data: ' + str(resend_data) + '\n')

        file.flush()
        os.fsync(file.fileno())
        summary.flush()
        os.fsync(summary.fileno())
        errors.flush()
        os.fsync(errors.fileno())

        host_check = check_host_is_correct(store_hostname)

        if host_check != True:
            continue

        total_qty = compare_total_qty(store_hostname, store_info['sbs_no'], store_info['store_no'])

        if total_qty == 'Offline':
            continue

        if total_qty == 'Not Equal':
            oracle_invn_list = query_oracle_invn_list(store_hostname, store_info['sbs_no'], store_info['store_no'])
            oracle_invn_list = dict(oracle_invn_list)
            oracle_invn_list = {key:int(value) for key, value in oracle_invn_list.items()}

            mysql_invn_list = query_mysql_invn_list(store_hostname, store_info['sbs_no'], store_info['store_no'])
            
            if mysql_invn_list == 'Offline':
                continue

            mysql_invn_list = dict(mysql_invn_list)
            mysql_invn_list = {key:int(value) for key, value in mysql_invn_list.items()}
            
            compare_lists(store_hostname, store_info['sbs_no'], store_info['store_no'], oracle_invn_list, mysql_invn_list, resend_data)

            if compare_lists == 'Offline':
                continue

        file.close()
        summary.close()
        errors.close()
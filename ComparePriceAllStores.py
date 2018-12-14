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
        errors.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        file.close()
        errors.close()
        summary.close()
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
        errors.write('Store Hostname: ' + store_hostname + ' does not match with active controller record address: ' + address + '\n\n')
        summary.close()
        errors.close()
        file.close()
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
    sbs_no = sbs_no[0]
    # print(sbs_no)
    cursor.execute(get_store_no)
    store_no = cursor.fetchone()
    store_no = store_no[0]
    # print(store_no)
    cursor.close()
    dbconnection.close()

    return {'sbs_no':sbs_no, 'store_no':store_no}

def query_oracle_total_price(store_hostname, sbs_no, store_no):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_total_price = """select sum(price) from cms.invn_sbs_price
                       where sbs_no = """ + str(sbs_no) + """
                       and item_sid in (select item_sid from cms.invn_sbs where active = 1 and sbs_no = """ + str(sbs_no) + """)"""

    cursor.execute(get_total_price)
    oracle_total_price = cursor.fetchone()
    oracle_total_price = '%.2f'%(oracle_total_price[0])

    cursor.close()
    dbconnection.close()

    return oracle_total_price

def query_mysql_total_price(store_hostname, sbs_no, store_no):
    # Open database connection
    try:
        db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')
    except:
        print('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database.')
        file.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n')
        summary.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        errors.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        errors.close()
        file.close()
        summary.close()
        return 'Offline'
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    cursor.execute("""select replace(format(sum(price), 2), ',', '') from rpsods.invn_sbs_price 
                      where invn_sbs_item_sid in (select sid from rpsods.invn_sbs_item where active = 1 and sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """)) 
                      and sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """) 
                      ;""")

    # Fetch a single row using fetchone() method.
    mysql_total_price = cursor.fetchone()
    mysql_total_price = mysql_total_price[0]
    

    # disconnect from server
    db.close()
    
    return mysql_total_price

def compare_total_price(store_hostname, sbs_no, store_no):
    oracle_total_price = query_oracle_total_price(store_hostname, sbs_no, store_no)
    mysql_total_price = query_mysql_total_price(store_hostname, sbs_no, store_no)

    if mysql_total_price == 'Offline':
        return 'Offline'

    if oracle_total_price == mysql_total_price:
        print('POA Price: ' + str(oracle_total_price))
        print(store_hostname + ' Price:' + str(mysql_total_price))
        print('Equal.')
        file.write('POA Price: ' + str(oracle_total_price) + '\n')
        summary.write('POA Price: ' + str(oracle_total_price) + '\n')
        file.write(store_hostname + ' Price: ' + str(mysql_total_price) + '\n')
        summary.write(store_hostname + ' Price: ' + str(mysql_total_price) + '\n')
        file.write('Equal. \n')
        summary.write('Equal. \n\n')
        return('Equal')

    if oracle_total_price != mysql_total_price:
        print('POA Price: ' + str(oracle_total_price))
        print(store_hostname + ' Price: ' + str(mysql_total_price))
        print('Not Equal.')
        file.write('POA Price: ' + str(oracle_total_price) + '\n')
        summary.write('POA Price: ' + str(oracle_total_price) + '\n')
        file.write(store_hostname + ' Price: ' + str(mysql_total_price) + '\n')
        summary.write(store_hostname + ' Price: ' + str(mysql_total_price) + '\n')
        file.write('Not Equal.\n')
        summary.write('Not Equal. \n\n')
        return('Not Equal')

def query_oracle_invn_list(store_hostname, sbs_no, store_no):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_invn_list = """select item_sid, price_lvl, (select to_char(price, 'fm999999999990.00') from dual) from cms.invn_sbs_price 
                       where sbs_no = """ + str(sbs_no) + """
                       and item_sid in (select item_sid from cms.invn_sbs where active = 1 and sbs_no = """ + str(sbs_no) + """)
                       order by item_sid, price_lvl"""
    
    print('Getting POA list of inventory...')
    cursor.execute(get_invn_list)
    oracle_invn_list = cursor.fetchall()

    cursor.close()
    dbconnection.close()

    return oracle_invn_list
    
def query_mysql_invn_list(store_hostname, sbs_no, store_no):
    # Open database connection
    try:
        db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')
    except:
        print('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database.')
        file.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n')
        errors.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        summary.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        file.close()
        errors.close()
        summary.close()
        return 'Offline'
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    print('Getting ' + store_hostname + ' list of inventory...')
    # execute SQL query using execute() method.
    cursor.execute("""select b.invn_item_uid, c.price_lvl, replace(format(a.price, 2), ',', '') 
                      from rpsods.invn_sbs_price as a 
                      left join rpsods.invn_sbs_item as b on a.invn_sbs_item_sid = b.sid
                      left join rpsods.price_level as c on a.price_lvl_sid = c.sid
                      where a.invn_sbs_item_sid in (select sid from rpsods.invn_sbs_item where active = 1 and sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """)) 
                      and a.sbs_sid in (select sid from rpsods.subsidiary where sbs_no = """ + str(sbs_no) + """) 
                      order by b.invn_item_uid, c.price_lvl;""")

    # Fetch a single row using fetchone() method.
    mysql_invn_list = cursor.fetchall()
    #mysql_invn_list = (mysql_total_qty[0])
    

    # disconnect from server
    db.close()
    
    return mysql_invn_list

def compare_lists(store_hostname, sbs_no, store_no, oracle_invn_dict, mysql_invn_dict, resend_data):
    print('Comparing prices between POA and store ' + store_hostname + '...')
    #matches = []
    store_indexed = {}
    poa_indexed = {}

    for store_record in mysql_invn_dict:
        store_indexed[(store_record["invn_sid"], store_record["price_lvl"], store_record["price"])] = store_record

    for poa_record in oracle_invn_dict:
        poa_indexed[(poa_record["invn_sid"], poa_record["price_lvl"], poa_record["price"])] = poa_record

    for poa_record in oracle_invn_dict:
        if (poa_record["invn_sid"], poa_record["price_lvl"], poa_record["price"]) not in store_indexed:
            print('Item SID: ' + str(poa_record['invn_sid']) + ' Price Level: ' + str(poa_record['price_lvl']) + ' differance found at ' + store_hostname + '.')
            file.write('Item SID: ' + str(poa_record['invn_sid']) + ' Price Level: ' + str(poa_record['price_lvl']) + ' differance found at ' + store_hostname + '.\n')
            rep_check_oracle = oracle_rep_check(sbs_no, store_no, str(poa_record['invn_sid']), str(poa_record['price_lvl']))
            #print('rep_check_oracle = ' + str(rep_check_oracle))
            #file.write('rep_check_oracle = ' + str(rep_check_oracle) + '\n')
            if rep_check_oracle != True:
                if resend_data == True:
                    print('Resending data...')
                    file.write('Resending data...' + '\n')
                    investigate.write('Item SID: ' + str(poa_record['invn_sid']) + ' Price Level: ' + str(poa_record['price_lvl']) + ' differance found at ' + store_hostname + '. Item is not waiting to be replicated.\n')
                    resend_item_v9(str(poa_record['invn_sid']), sbs_no, store_no)
            
            
            for store_record in mysql_invn_dict: 
                if poa_record["invn_sid"] == store_record["invn_sid"] and poa_record["price_lvl"] == store_record["price_lvl"]:
                    print('Item SID: ' + str(poa_record['invn_sid']) + ' Price Level: ' + str(poa_record['price_lvl']) +  ' price not equal. POA Price: ' + str(poa_record['price']) + ' ' + store_hostname + ' Price: ' + str(store_record['price']))
                    file.write('Item SID: ' + str(poa_record['invn_sid']) + ' Price Level: ' + str(poa_record['price_lvl']) +  ' price not equal. POA Price: ' + str(poa_record['price']) + ' ' + store_hostname + ' Price: ' + str(store_record['price']) + '\n')

    #for store_item in mysql_invn_dict:
    #    if (store_item["invn_sid"], store_item["price_lvl"]) not in poa_indexed:
    #        print('Item SID: ' + str(store_record['invn_sid']) + ' Price Level: ' + str(store_record['price_lvl']) + ' not found at POA.')
    #        file.write('Item SID: ' + str(store_record['invn_sid']) + ' Price Level: ' + str(store_record['price_lvl']) + ' not found at POA.\n')

def oracle_rep_check(sbs_no, store_no, key, price_level):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    drs_rep_check = """select count(*) from drs.drs_invn_sbs_price 
                    where item_sid = """ + str(key) + """ and sbs_no = """ + str(sbs_no) + """ and price_lvl = """ + str(price_level) + """""" 
        
    
    cursor.execute(drs_rep_check)
    oracle_drs_rep_check = cursor.fetchone()

    pub_rep_check = """select count(*) from drs.pub_invn_sbs_qty 
                    where item_sid = """ + str(key) + """ and sbs_no = """ + str(sbs_no) + """ and store_no = """ + str(price_level) + """""" 
    
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

        if not os.path.exists('Price_' + str(time_and_date)):
            os.makedirs('Price_' + str(time_and_date))

        errors = open('Price_' + str(time_and_date) + '\\Errors_Price_' + time_and_date + '.txt', 'a')
        summary = open('Price_' + str(time_and_date) + '\\Summary_Price_' + time_and_date + '.txt', 'a')
        file = open('Price_' + str(time_and_date) + '\\' + store_hostname + '_Price_' + time_and_date + '.txt', 'w')
        investigate = open('Price_' + str(time_and_date) + '\\Investigate_Price_' + time_and_date + '.txt', 'a')
        
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
        investigate.flush()
        os.fsync(investigate.fileno())

        host_check = check_host_is_correct(store_hostname)

        if host_check != True:
            continue
        
        #total_price = compare_total_price(store_hostname, store_info['sbs_no'], store_info['store_no'])

        #if total_price == 'Offline':
        #    continue

        #if total_price == 'Not Equal':
        oracle_invn_list = query_oracle_invn_list(store_hostname, store_info['sbs_no'], store_info['store_no'])
        oracle_invn_dict = []
        for record in oracle_invn_list:
            oracle_invn_dict.append(dict(invn_sid = record[0], price_lvl = record[1], price = record[2]))

        mysql_invn_list = query_mysql_invn_list(store_hostname, store_info['sbs_no'], store_info['store_no'])
        
        if mysql_invn_list == 'Offline':
            continue

        mysql_invn_dict = []
        for record in mysql_invn_list:
            mysql_invn_dict.append(dict(invn_sid = record[0], price_lvl = record[1], price = record[2]))
        
        compare_lists(store_hostname, store_info['sbs_no'], store_info['store_no'], oracle_invn_dict, mysql_invn_dict, resend_data)

        if compare_lists == 'Offline':
            continue

        file.close()
        summary.close()
        errors.close()

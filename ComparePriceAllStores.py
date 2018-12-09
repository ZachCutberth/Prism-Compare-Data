# Compare inventory Price/Qty between POA and store servers.
# By Zach Cutberth

import cx_Oracle
import pymysql
import config
import os
import time

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
    sbs_no = sbs_no[0]
    # print(sbs_no)
    cursor.execute(get_store_no)
    store_no = cursor.fetchone()
    store_no = store_no[0]
    # print(store_no)
    cursor.close()
    dbconnection.close()

    return {'sbs_no':sbs_no, 'store_no':store_no}

def query_oracle_invn_list(store_hostname, sbs_no, store_no):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_invn_list = """select item_sid, price_lvl, (select to_char(price, 'fm999999999990.00') from dual) from cms.invn_sbs_price 
                       where sbs_no = """ + str(sbs_no) + """
                       and item_sid in (select item_sid from cms.invn_sbs where active = 1 and sbs_no = """ + str(sbs_no) + """)
                       order by item_sid"""
    
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
        file.close()
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
                      group by b.invn_item_uid;""")

    # Fetch a single row using fetchone() method.
    mysql_invn_list = cursor.fetchall()
    #mysql_invn_list = (mysql_total_qty[0])
    

    # disconnect from server
    db.close()
    
    return mysql_invn_list

def compare_lists(store_hostname, sbs_no, store_no, oracle_invn_dict, mysql_invn_dict, resend_data):
    print('Comparing prices between POA and store ' + store_hostname + '...')
    for poa_record in oracle_invn_dict:
        print('POA Record: ' + str(poa_record))
        store_record = next((store_record for store_record in mysql_invn_dict if store_record['invn_sid'] == poa_record['invn_sid'] and store_record['price_lvl'] == poa_record['price_lvl']), False)
        print('Store Record: ' + str(store_record))
        if store_record == False:
            print('Item SID: ' + str(poa_record['invn_sid']) + ' not found at ' + store_hostname + '.')
            file.write('Item SID: ' + str(poa_record['invn_sid']) + ' not found at ' + store_hostname + '.\n')
            rep_check_oracle = oracle_rep_check(sbs_no, store_no, str(poa_record['invn_sid']), str(poa_record['price_lvl']))
            print('rep_check_oracle = ' + str(rep_check_oracle))
            file.write('rep_check_oracle = ' + str(rep_check_oracle) + '\n')
            if rep_check_oracle != True:
                if resend_data == True:
                    print('Resending data...')
                    file.write('Resending data...' + '\n')
                    resend_item_v9(str(poa_record['invn_sid']), sbs_no, store_no)
            continue
        if str(poa_record['invn_sid']) == str(store_record['invn_sid']):
            if str(poa_record['price_lvl']) == str(store_record['price_lvl']):
                if str(poa_record['price']) != str(store_record['price']):
                    print('Item SID: ' + str(poa_record['invn_sid']) + ' Price Level: ' + str(poa_record['price_lvl']) +  ' price not equal. POA Price: ' + str(poa_record['price']) + ' ' + store_hostname + ' Price: ' + str(store_record['price']))
                    file.write('Item SID: ' + str(poa_record['invn_sid']) + ' Price Level: ' + str(poa_record['price_lvl']) +  ' price not equal. POA Price: ' + str(poa_record['price']) + ' ' + store_hostname + ' Price: ' + str(store_record['price']) + '\n')
                    rep_check_oracle = oracle_rep_check(sbs_no, store_no, str(poa_record['invn_sid']), str(poa_record['price_lvl']))
                    print('rep_check_oracle = ' + str(rep_check_oracle))
                    file.write('rep_check_oracle = ' + str(rep_check_oracle) + '\n')
                    if rep_check_oracle != True:
                        if resend_data == True:
                            print('Resending data...')
                            file.write('Resending data...' + '\n')
                            resend_item_v9(str(poa_record['invn_sid']), sbs_no, store_no)

    for store_record in mysql_invn_dict:
        poa_record = next((poa_record for poa_record in oracle_invn_dict if poa_record['invn_sid'] == store_record['invn_sid'] and poa_record['price_lvl'] == store_record['price_lvl']), False)
        if poa_record == False:                
            print('Item SID: ' + str(store_record['invn_sid']) + ' not found at POA.')
            file.write('Item SID: ' + str(store_record['invn_sid']) + ' not found at POA.\n')

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

store_list = [
    {'store_hostname': 'kkm-1', 'store_code': 'KKM'},
    {'store_hostname': 'pve-1', 'store_code': 'PVE'},
    {'store_hostname': 'ggj-1', 'store_code': 'GGJ'},
    {'store_hostname': 'dpm-1', 'store_code': 'DPM'},
    {'store_hostname': 'scj-1', 'store_code': 'SCJ'},
    {'store_hostname': 'spm-1', 'store_code': 'SPM'},
    {'store_hostname': 'pue-1', 'store_code': 'PUE'},
    {'store_hostname': 'kve-1', 'store_code': 'KVE'},
    {'store_hostname': 'cwj-1', 'store_code': 'CWJ'},
    {'store_hostname': 'bbm-1', 'store_code': 'BBM'},
    {'store_hostname': 'btm-1a', 'store_code': 'BTM'},
    {'store_hostname': 'kmm-1', 'store_code': 'KMM'},
    {'store_hostname': 'mtm-1', 'store_code': 'MTM'},
    {'store_hostname': 'pio-1', 'store_code': 'PIO'},
    {'store_hostname': 'tpo-1', 'store_code': 'TPO'},
    {'store_hostname': 'aye-1', 'store_code': 'AYE'},
    {'store_hostname': 'bpe-1', 'store_code': 'BPE'},
    {'store_hostname': 'bse-1', 'store_code': 'BSE'},
    {'store_hostname': 'bxe-1', 'store_code': 'BXE'},
    {'store_hostname': 'cpe-1', 'store_code': 'CPE'},
    {'store_hostname': 'gre-1a', 'store_code': 'GRE'},
    {'store_hostname': 'kke-1', 'store_code': 'KKE'},
    {'store_hostname': 'mte-1', 'store_code': 'MTE'},
    {'store_hostname': 'pee-1', 'store_code': 'PEE'},
    {'store_hostname': 'pie-1', 'store_code': 'PIE'},
    {'store_hostname': 'ppe-1a', 'store_code': 'PPE'},
    {'store_hostname': 'sbe-1', 'store_code': 'SBE'},
    {'store_hostname': 'sce-1', 'store_code': 'SCE'},
    {'store_hostname': 'sme-1', 'store_code': 'SME'},
    {'store_hostname': 'tae-1', 'store_code': 'TAE'},
    {'store_hostname': 'tme-1', 'store_code': 'TME'},
    {'store_hostname': 'kgj-1', 'store_code': 'KGJ'},
    {'store_hostname': 'kvj-1', 'store_code': 'KVJ'},
    {'store_hostname': 'pij-1', 'store_code': 'PIJ'},
    {'store_hostname': 'pvj-1', 'store_code': 'PVJ'},
   # {'store_hostname': 'suj-1', 'store_code': 'SUJ'},
    {'store_hostname': 'tpj-1', 'store_code': 'TPJ'},
    {'store_hostname': 'bdm-1a', 'store_code': 'BDM'},
    {'store_hostname': 'cpj-1', 'store_code': 'CPJ'},
    {'store_hostname': 'gio-1', 'store_code': 'GIO'},
    {'store_hostname': 'pkj-1', 'store_code': 'PKJ'},
    {'store_hostname': 'pem-1', 'store_code': 'PEM'},
    {'store_hostname': 'rim-1', 'store_code': 'RIM'},
    {'store_hostname': 't3m-1a', 'store_code': 'T3M'},
    {'store_hostname': 'aym-1', 'store_code': 'AYM'},
    {'store_hostname': 'ppm-1', 'store_code': 'PPM'},
    {'store_hostname': 'bxm-1', 'store_code': 'BXM'},
    {'store_hostname': 'cmm-1', 'store_code': 'CMM'},
    {'store_hostname': 'skm-1', 'store_code': 'SKM'},
    {'store_hostname': 'kam-1', 'store_code': 'KAM'},
    {'store_hostname': 'pim-1', 'store_code': 'PIM'},
    {'store_hostname': 'pum-1', 'store_code': 'PUM'},
    {'store_hostname': 'cpm-1', 'store_code': 'CPM'},
    {'store_hostname': 'pvm-1', 'store_code': 'PVM'},
    {'store_hostname': 'tsm-1', 'store_code': 'TSM'},
    {'store_hostname': 'bmm-1', 'store_code': 'BMM'},
    {'store_hostname': 'plm-1', 'store_code': 'PLM'},
    {'store_hostname': 'lpm-1', 'store_code': 'LPM'},
    {'store_hostname': 'gam-1', 'store_code': 'GAM'},
    {'store_hostname': 'pbm-1', 'store_code': 'PBM'},
    {'store_hostname': 'pkm-1', 'store_code': 'PKM'},
    {'store_hostname': 'smm-1', 'store_code': 'SMM'},
    {'store_hostname': 'hmm-1', 'store_code': 'HMM'},
    {'store_hostname': 'tmm-1', 'store_code': 'TMM'},
    {'store_hostname': 'bpm-1', 'store_code': 'BPM'},
    {'store_hostname': 'sum-1a', 'store_code': 'SUM'},
    {'store_hostname': 'pmm-1a', 'store_code': 'PMM'},
    {'store_hostname': 'scm-1', 'store_code': 'SCM'},
    {'store_hostname': 'kvm-1', 'store_code': 'KVM'},
    {'store_hostname': 'phm-1', 'store_code': 'PHM'},
    {'store_hostname': 'gim-1a', 'store_code': 'GIM'},
    {'store_hostname': 'klm-1', 'store_code': 'KLM'},
    {'store_hostname': 'pve-1', 'store_code': 'PVE'},
    {'store_hostname': 'sbm-1', 'store_code': 'SBM'},
    #{'store_hostname': 'tam-1a', 'store_code': 'TAM'},
    {'store_hostname': 'kgo-1', 'store_code': 'KGO'},
    {'store_hostname': 'plo-1', 'store_code': 'PLO'},
    {'store_hostname': 'cme-1', 'store_code': 'CME'},
    {'store_hostname': 'kle-1', 'store_code': 'KLE'},
    {'store_hostname': 'tse-1', 'store_code': 'TSE'},
    {'store_hostname': 'gij-1', 'store_code': 'GIJ'},
    {'store_hostname': 'kaj-1', 'store_code': 'KAJ'},
    {'store_hostname': 'kkj-1', 'store_code': 'KKJ'},
    {'store_hostname': 'lpj-1', 'store_code': 'LPJ'},
    {'store_hostname': 'phj-1', 'store_code': 'PHJ'},
    {'store_hostname': 'spj-1', 'store_code': 'SPJ'},
    #{'store_hostname': 'xxj-1', 'store_code': 'XXJ'}

]

for store in store_list:
    store_hostname = store['store_hostname']
    store_code = store['store_code']

    resend_data = 'y'

    if resend_data == 'y':
        resend_data = True
    else: 
        resend_data = False

    if not os.path.exists(str(time_and_date)):
        os.makedirs(str(time_and_date))

    summary = open(str(time_and_date) + '\\summary_' + time_and_date + '.txt', 'a')
  
    file = open(str(time_and_date) + '\\' + store_hostname + '_' + time_and_date + '.txt', 'w')
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

    host_check = check_host_is_correct(store_hostname)

    if host_check != True:
        continue
    
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

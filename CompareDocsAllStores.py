# Compare inventory Price/Qty between POA and store servers.
# By Zach Cutberth

import cx_Oracle
import pymysql
import config
import os
import time
from subprocess import Popen, PIPE
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
        summary.close()
        errors.close()
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
        errors.close()
        summary.close()
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

def query_oracle_total_docs(store_hostname, sbs_no, store_no):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_total_docs = """select count(invc_sid) from cms.invoice
                       where sbs_no = """ + str(sbs_no) + """ and store_no = """ + str(store_no) + """
                       and invc_type in (0, 2) and held = 0
                       """

    cursor.execute(get_total_docs)
    oracle_total_docs = cursor.fetchone()
    oracle_total_docs = oracle_total_docs[0]

    cursor.close()
    dbconnection.close()

    return oracle_total_docs

def query_mysql_total_docs(store_hostname, sbs_no, store_no):
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
    cursor.execute("""select count(sid) from rpsods.document
                      where status = 4 and order_type is null and is_held = 0
                      and sbs_no = """ + str(sbs_no) + """
                      and store_no = """ + str(store_no) + """;""")

    # Fetch a single row using fetchone() method.
    mysql_total_docs = cursor.fetchone()
    mysql_total_docs = mysql_total_docs[0]
    
    # disconnect from server
    db.close()
    
    return mysql_total_docs

def compare_total_docs(store_hostname, sbs_no, store_no):
    oracle_total_docs = query_oracle_total_docs(store_hostname, sbs_no, store_no)
    mysql_total_docs = query_mysql_total_docs(store_hostname, sbs_no, store_no)

    if mysql_total_docs == 'Offline':
        return 'Offline'

    if oracle_total_docs == mysql_total_docs:
        print('POA Docs: ' + str(oracle_total_docs))
        print(store_hostname + ' Docs:' + str(mysql_total_docs))
        print('Equal.')
        file.write('POA Docs: ' + str(oracle_total_docs) + '\n')
        summary.write('POA Docs: ' + str(oracle_total_docs) + '\n')
        file.write(store_hostname + ' Docs: ' + str(mysql_total_docs) + '\n')
        summary.write(store_hostname + ' Docs: ' + str(mysql_total_docs) + '\n')
        file.write('Equal. \n')
        summary.write('Equal. \n\n')
        return('Equal')

    if oracle_total_docs != mysql_total_docs:
        print('POA Docs: ' + str(oracle_total_docs))
        print(store_hostname + ' Docs: ' + str(mysql_total_docs))
        print('Not Equal.')
        file.write('POA Docs: ' + str(oracle_total_docs) + '\n')
        summary.write('POA Docs: ' + str(oracle_total_docs) + '\n')
        file.write(store_hostname + ' Docs: ' + str(mysql_total_docs) + '\n')
        summary.write(store_hostname + ' Docs: ' + str(mysql_total_docs) + '\n')
        file.write('Not Equal.\n')
        summary.write('Not Equal. \n\n')
        return('Not Equal')

def query_oracle_doc_list(store_hostname, sbs_no, store_no):
    connstr = config.connstr
    dbconnection = cx_Oracle.connect(connstr)
    cursor = dbconnection.cursor()

    get_doc_list = """select invc_sid from cms.invoice
                       where sbs_no = """ + str(sbs_no) + """ and store_no = """ + str(store_no) + """
                       and invc_type in (0, 2) and held = 0
                       """
    
    print('Getting POA list of docs...')
    cursor.execute(get_doc_list)
    oracle_doc_list = cursor.fetchall()

    cursor.close()
    dbconnection.close()
    #print(oracle_doc_list)
    return oracle_doc_list
    
def query_mysql_doc_list(store_hostname, sbs_no, store_no):
    # Open database connection
    try:
        db = pymysql.connect(store_hostname, config.mysql_user, config.mysql_pass,'rpsods')
    except:
        print('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database.')
        file.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n')
        errors.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        summary.write('Store Hostname: ' + store_hostname + ' - Could not connect to MySQL database. \n\n')
        summary.close()
        errors.close()
        file.close()
        return 'Offline'
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    print('Getting ' + store_hostname + ' list of docs...')
    # execute SQL query using execute() method.
    cursor.execute("""select sid from rpsods.document
                      where status = 4 and order_type is null and is_held = 0
                      and sbs_no = """ + str(sbs_no) + """
                      and store_no = """ + str(store_no) + """;""")

    # Fetch a single row using fetchone() method.
    mysql_doc_list = cursor.fetchall()
    #mysql_doc_list = str(mysql_doc_list).replace('()', '')
    #print(mysql_doc_list)
    #mysql_invn_list = (mysql_total_qty[0])
    

    # disconnect from server
    db.close()
    
    return mysql_doc_list

def compare_lists(store_hostname, sbs_no, store_no, oracle_doc_list, mysql_doc_list, resend_data):
    print('Comparing docs between POA and store ' + store_hostname + '...')
    
    for store_record in mysql_doc_list:
        if store_record not in oracle_doc_list:
            print('Item Doc: ' + str(store_record[0]) + ' not found at POA.')
            file.write('Item Doc: ' + str(store_record[0]) + ' not found at POA. \n')
            #rep_check_oracle = oracle_rep_check(sbs_no, store_no, str(poa_record['invn_sid']), str(poa_record['price_lvl']))
            #print('rep_check_oracle = ' + str(rep_check_oracle))
            #file.write('rep_check_oracle = ' + str(rep_check_oracle) + '\n')
            #if rep_check_oracle != True:
            if resend_data == True:
                print('Resending data...')
                file.write('Resending data...' + '\n')
                resend_doc(str(store_record[0]), sbs_no, store_no)

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

def resend_doc(doc_sid, sbs_no, store_no):
    
    select_statement = """declare cRec cursor for
                                select t.sid, t.controller_sid from rpsods.document t where t.status = 4 and t.sid in (""" + str(doc_sid) + """);"""

      

    sql = """
    -- NOTE: this script is for Prism 1.10 or higher - it will NOT work on older version

    -- drop procedure resend_script;
    drop procedure if exists resend_script;
    SET SQL_SAFE_UPDATES = 0;
    delimiter $$
    create procedure resend_script()
    begin
        declare from_date date default date'2018-01-01';
        declare to_date date   default date'2018-01-01';
        -- list of resource names could be found in resource_name column of rpsods.prism_resource table
        -- IMPORTANT: MUST BE IN LOWER CASE
        declare res_name varchar(50) default 'document';
        declare inserted int;
        declare deSID bigint;
        declare res_namespace varchar(100);
        declare rec_sid bigint;
        declare controller_sid bigint;

        -- loop by documents
        declare rec_loop_exit boolean;

        -- adjust this query to reflect what you want to re-send and what to filter it by

        """+ select_statement +"""

        declare continue handler for not found set rec_loop_exit = true;

        open cRec;

        document_loop: loop
            set inserted = 0;
            set deSID = 0;

            fetch cRec into rec_sid, controller_sid;
            if rec_loop_exit then
            close cRec;
            leave document_loop;
            end if;

            begin
            declare subscr_sid bigint;
            declare resource_name varchar(50);
            declare rps_entity_name varchar(50);
            declare subscr_contr_sid bigint;
            declare true_name varchar(50);
            declare res_sid bigint;
            declare res_namespace varchar(100);

            declare conn_loop_exit boolean;

            declare cConn cursor for
                select cs.sid, r.resource_name, r.rps_entity_name, cs.controller_sid,
                    coalesce(nullif(r.rps_entity_name, ''), r.resource_name) as true_res_name, r.sid as resource_sid
                from rpsods.rem_connection_subscr cs
                join rpsods.remote_connection c
                    on (cs.remote_connection_sid = c.sid)
                    and (c.active = 1)
                join rpsods.rem_subscription s
                    on (cs.subscription_sid = s.sid)
                    and (s.active = 1) and (s.subscription_type in (0, 1))
                join rpsods.rem_subscr_resource sr
                    on (s.sid = sr.rem_subscription_sid)
                join rpsods.prism_resource r
                    on (sr.prism_resource_sid = r.sid)
                    and lower(r.namespace) = 'replication'
                where lower(r.resource_name) = res_name;

            declare continue handler for not found set conn_loop_exit = true;

            open cConn;

            -- loop by connections for requested resource

            conn_loop: loop
                fetch cConn into subscr_sid, resource_name, rps_entity_name, subscr_contr_sid, true_name, res_sid;
                if conn_loop_exit then
                leave conn_loop;
                end if;

                set res_namespace = concat('/v1/rest/', res_name, '/');
                begin
                declare continue handler for 1329 begin end;
                select concat('/api/', t.namespace, '/', res_name, '/')
                into res_namespace
                from rpsods.prism_resource t
                where lower(t.resource_name) = res_name
                    and lower(t.namespace) <> 'replication'
                limit 1;
                end;

                -- insert data event record (one time only)
                if inserted = 0 then
                set deSID = GetSid();
                insert into rpsods.pub_dataevent_queue
                    (sid, created_by, created_datetime, controller_sid, origin_application, row_version,
                    resource_name, resource_sid, link, event_type,
                    attributes_affected, prism_resource_sid)
                values
                    (deSID, 'PUBSUB', sysdate(), controller_sid, 'ReSendScript', 1,
                    upper(true_name), rec_sid, concat(res_namespace, rec_sid), 2,
                    'ROW_VERSION,Status', res_sid);
                set inserted = inserted + 1;
                end if;

                -- insert data notification record (one per subscriber)
                insert into rpsods.pub_notification_queue
                (sid, created_by, created_datetime, controller_sid,
                origin_application, row_version, dataevent_queue_sid, subscription_event_sid, transmit_status)
                values
                (GetSid(), 'PUBSUB', sysdate(), controller_sid,
                'ReSendScript', 1, deSID, subscr_sid, 0);

            end loop conn_loop;

            close cConn;
        end;
        end loop document_loop;
        -- loop by documents
        commit;
    end;
    $$
    delimiter ;
    call resend_script();
    drop procedure resend_script;

    -- SET SQL_SAFE_UPDATES = 1;                                  
    """
    sql_file = open('resend.sql', 'w')
    sql_file.write(sql)
    sql_file.close()
    path = os.getcwd()
    mysql_exe = '\"' + 'c:\\rpsupport\\mysql-5.7.24-win32\\bin\\mysql.exe' + '\"' 
    Popen(mysql_exe + ' -u' + config.mysql_user + ' -p' + config.mysql_pass + ' -h ' + store_hostname + ' rpsods < "' + path + '\\resend.sql"', shell=True).communicate()
    os.remove('resend.sql')


    print('Doc SID: ' + str(doc_sid) + ' has been queued for replication from the store to the POA.')
    file.write('Doc SID: ' + str(doc_sid) + ' has been queued for replication from the store to the POA.\n')


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

        if not os.path.exists('Doc_' + str(time_and_date)):
            os.makedirs('Doc_' + str(time_and_date))

        summary = open('Doc_' + str(time_and_date) + '\\Doc_Summary_' + time_and_date + '.txt', 'a')
        errors = open('Doc_' + str(time_and_date) + '\\Doc_Errors_' + time_and_date + '.txt', 'a')
        file = open('Doc_' + str(time_and_date) + '\\Doc_' + store_hostname + '_' + time_and_date + '.txt', 'w')
        
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
        
        total_docs = compare_total_docs(store_hostname, store_info['sbs_no'], store_info['store_no'])

        if total_docs == 'Offline':
            continue

        if total_docs == 'Not Equal':
            oracle_doc_list = query_oracle_doc_list(store_hostname, store_info['sbs_no'], store_info['store_no'])

            mysql_doc_list = query_mysql_doc_list(store_hostname, store_info['sbs_no'], store_info['store_no'])
            
            if mysql_doc_list == 'Offline':
                continue
            
            compare_lists(store_hostname, store_info['sbs_no'], store_info['store_no'], oracle_doc_list, mysql_doc_list, resend_data)

            if compare_lists == 'Offline':
                continue

        file.close()
        summary.close()
        errors.close()

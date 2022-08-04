import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import pandas as pd
from io import StringIO
import sys
from pprint import pprint


# define a function that will execute SQL statements
def execute_sql(cursor, statement, ident=None):

    print ("\nexecute_sql() SQL statement:", statement)

    # check if SQL statement/query end with a semi-colon
    if statement[-1] != ";":
        sql_err = "execute_sql() ERROR: All SQL statements and " \
              "queries must end with a semi-colon."
        print (sql_err)
    else:
        try:
            # have sql.SQL() return a sql.SQL object
            sql_object = sql.SQL(
            # pass SQL string to sql.SQL()
            statement
            ).format(
            # pass the identifier to the Identifier() method
            sql.Identifier( ident )
            )

            # pass the psycopg2.sql.SQL object to execute() method
            cursor.execute( sql_object )

            # print message if no exceptions were raised
            print("execute_sql() FINISHED")

        except Exception as err:
            print("execute_sql() ERROR:", err)


def dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    ps_conn = psycopg2.connect(**credentials)
    ps_conn.autocommit = True  # auto-commit each entry to the database
    #   ps_conn.cursor_factory = RealDictCursor
    ps_cur = ps_conn.cursor()
    print ("Connected Successfully to DB: " + str(db_parm) + "@" + str(host_parm))
    return ps_conn, ps_cur


def createTable(cur, table_name):
    sql_statement = """
    DROP TABLE IF EXISTS {}
    ;""".format(table_name)
    #execute_sql(cur, sql_statement, ident='table_name')

    sql_statement = """
    CREATE TABLE {} (
    mp_log_id SERIAL PRIMARY KEY,
    order_uuid VARCHAR(32) NOT NULL,
    order_id VARCHAR(32) NOT NULL,
    account_list_id INTEGER NOT NULL,
    log_date timestamp NOT NULL DEFAULT NOW()
    );""".format(table_name)

    # print the psycopg2.sql.SQL object
    print("CREATE TABLE sql_statement:\n", sql_statement)

    # call the function to create table
    #execute_sql(cur, sql_statement, ident='table_name')

    table_name2 ='orders'
    sql_statement = """
    DROP TABLE IF EXISTS {}
    ;""".format(table_name2)
    execute_sql(cur, sql_statement, ident='table_name2')

    sql_statement = """
    CREATE TABLE {} (
    order_uuid VARCHAR(32) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    order_number VARCHAR(32),
    posting_number VARCHAR(32),
    status VARCHAR(32),
    delivery_method_id BIGINT,
    delivery_method_name TEXT,
    delivery_method_warehouse_id BIGINT,
    delivery_method_warehouse TEXT,
    delivery_method_tpl_provider_id INTEGER,
    delivery_method_tpl_provider TEXT,
    tracking_number TEXT,
    tpl_integration_type TEXT,
    created_at TIMESTAMP,
    in_process_at TIMESTAMP,
    shipment_date TIMESTAMP,
    delivering_date TIMESTAMP,
    cancellation_cancel_reason_id INTEGER,
    cancellation_cancel_reason TEXT,
    cancellation_cancellation_type TEXT,
    cancellation_cancelled_after_ship BOOL,
    cancellation_affect_cancellation_rating BOOL,
    cancellation_cancellation_initiator TEXT,
    customer_customer_id BIGINT,
    customer_customer_email TEXT,
    customer_phone TEXT,
    customer_address_address_tail TEXT,
    customer_address_city TEXT,
    customer_address_comment TEXT,
    customer_address_country TEXT,
    customer_address_district TEXT,
    customer_address_region TEXT,
    customer_address_zip_code TEXT,
    customer_address_latitude REAL,
    customer_address_longitude REAL,
    customer_address_pvz_code INTEGER,
    customer_address_provider_pvz_code TEXT,
    customer_name TEXT,
    addressee_name TEXT,
    addressee_phone TEXT,
    barcodes TEXT,
    analytics_data_region TEXT,
    analytics_data_city TEXT,
    analytics_data_delivery_type TEXT,
    analytics_data_is_premium BOOLEAN,
    analytics_data_payment_type_group_name TEXT,
    analytics_data_warehouse_id TEXT,
    analytics_data_warehouse TEXT,
    analytics_data_tpl_provider_id INT,
    analytics_data_tpl_provider TEXT,
    analytics_data_delivery_date_begin TIMESTAMP,
    analytics_data_delivery_date_end TIMESTAMP,
    analytics_data_is_legal BOOL,
    fd_posting_services_marketplace_service_item_fulfillment REAL,
    fd_posting_services_marketplace_service_item_pickup REAL,
    fd_posting_services_marketplace_service_item_dropoff_pvz REAL,
    fd_posting_services_marketplace_service_item_dropoff_sc REAL,
    fd_posting_services_marketplace_service_item_dropoff_ff REAL,
    fd_posting_services_marketplace_service_item_direct_flow_trans REAL,
    fd_posting_services_marketplace_service_item_return_flow_trans REAL,
    fd_posting_services_marketplace_service_item_deliv_to_customer REAL,
    fd_posting_services_marketplace_service_item_return_not_deliv_to_customer REAL,
    fd_posting_services_marketplace_service_item_return_part_goods_customer REAL,
    fd_posting_services_marketplace_service_item_return_after_deliv_to_customer REAL,
    additional_data TEXT,
    is_express BOOL,
    requirements_products_requiring_gtd TEXT,
    requirements_products_requiring_country TEXT,
    requirements_products_requiring_mandatory_mark TEXT,
    requirements_products_requiring_rnpt TEXT,
    parent_posting_number TEXT
    );""".format(table_name2)
    #sku,name,quantity,offer_id,price,digital_codes,order_id,
    # fd_commission_amount REAL,
    # fd_commission_percent REAL,
    # fd_payout REAL,
    # fd_product_id,
    # fd_old_price REAL,
    # fd_price REAL,
    # fd_total_discount_value REAL,
    # fd_total_discount_percent REAL,
    # fd_actions,
    # fd_picking,
    # fd_quantit REALy,
    # fd_client_price REAL,
    # fd_item_services_marketplace_service_item_fulfillment REAL,
    # fd_item_services_marketplace_service_item_pickup REAL,
    # fd_item_services_marketplace_service_item_dropoff_pvz REAL,
    # fd_item_services_marketplace_service_item_dropoff_sc REAL,
    # fd_item_services_marketplace_service_item_dropoff_ff REAL,
    # fd_item_services_marketplace_service_item_direct_flow_trans REAL,
    # fd_item_services_marketplace_service_item_return_flow_trans REAL,
    # fd_item_services_marketplace_service_item_deliv_to_customer REAL,
    # fd_item_services_marketplace_service_item_return_not_deliv_to_customer REAL,
    # fd_item_services_marketplace_service_item_return_part_goods_customer REAL,
    # fd_item_services_marketplace_service_item_return_after_deliv_to_customer REAL,
    # order_number,posting_number,status,cancel_reason_id,created_at,in_process_at REAL,
    # products,
    # region,city,delivery_type,is_premium,payment_type_group_name,warehouse_id,warehouse_name,is_legal,
    # marketplace_service_item_fulfillment REAL,
    # marketplace_service_item_pickup REAL,
    # marketplace_service_item_dropoff_pvz REAL,
    # marketplace_service_item_dropoff_sc REAL,
    # marketplace_service_item_dropoff_ff REAL,
    # marketplace_service_item_direct_flow_trans REAL,
    # marketplace_service_item_return_flow_trans REAL,
    # marketplace_service_item_deliv_to_customer REAL,
    # marketplace_service_item_return_not_deliv_to_customer REAL,
    # marketplace_service_item_return_part_goods_customer REAL,
    # marketplace_service_item_return_after_deliv_to_customer REAL,
    # additional_data
    #"digital_codes": [ ]
    #    fd_actions,



    #price, offer_id, name_x, sku, quantity, mandatory_mark, currency_code,  fd_commission_amount, fd_commission_percent, fd_payout, fd_product_id, fd_old_price, fd_price, fd_total_discount_value, fd_total_discount_percent, fd_actions, fd_picking, fd_quantity, fd_client_price, fd_item_services_marketplace_service_item_fulfillment, fd_item_services_marketplace_service_item_pickup, fd_item_services_marketplace_service_item_dropoff_pvz, fd_item_services_marketplace_service_item_dropoff_sc, fd_item_services_marketplace_service_item_dropoff_ff, fd_item_services_marketplace_service_item_direct_flow_trans, fd_item_services_marketplace_service_item_return_flow_trans, fd_item_services_marketplace_service_item_deliv_to_customer, fd_item_services_marketplace_service_item_return_not_deliv_to_customer, fd_item_services_marketplace_service_item_return_part_goods_customer, fd_item_services_marketplace_service_item_return_after_deliv_to_customer, posting_number, order_number, status, id, name_y, warehouse_id, warehouse, tpl_provider_id, tpl_provider, tracking_number, tpl_integration_type, in_process_at, shipment_date, delivering_date, cancel_reason_id, cancel_reason, cancellation_type, cancelled_after_ship, affect_cancellation_rating, cancellation_initiator, customer, products, addressee, barcodes, region, city, delivery_type, is_premium, payment_type_group_name, delivery_date_begin, delivery_date_end, is_legal, marketplace_service_item_fulfillment, marketplace_service_item_pickup, marketplace_service_item_dropoff_pvz, marketplace_service_item_dropoff_sc, marketplace_service_item_dropoff_ff, marketplace_service_item_direct_flow_trans, marketplace_service_item_return_flow_trans, marketplace_service_item_deliv_to_customer, marketplace_service_item_return_not_deliv_to_customer, marketplace_service_item_return_part_goods_customer, marketplace_service_item_return_after_deliv_to_customer, is_express, products_requiring_gtd, products_requiring_country, products_requiring_mandatory_mark, products_requiring_rnpt, parent_posting_number

    # print the psycopg2.sql.SQL object
    print("CREATE TABLE sql_statement:\n", sql_statement)

    # call the function to create table
    execute_sql(cur, sql_statement, ident='table_name2')

    table_name3 ='products'
    sql_statement = """
    DROP TABLE IF EXISTS {}
    ;""".format(table_name3)
    execute_sql(cur, sql_statement, ident='table_name2')

    sql_statement = """
    CREATE TABLE {} (
    order_uuid VARCHAR(32) NOT NULL,
    sku BIGINT,
    name VARCHAR(256),
    quantity INTEGER DEFAULT 1,
    offer_id VARCHAR(32),
    price REAL,
    mandatory_mark TEXT,
    currency_code TEXT,
    digital_codes TEXT,
    fd_commission_amount REAL,
    fd_commission_percent REAL,
    fd_payout REAL,
    fd_product_id BIGINT,
    fd_old_price REAL,
    fd_price REAL,
    fd_total_discount_value REAL,
    fd_total_discount_percent REAL,
    fd_actions TEXT,
    fd_picking INTEGER,
    fd_quantity INTEGER,
    fd_client_price TEXT,
    fd_item_services_marketplace_service_item_fulfillment REAL,
    fd_item_services_marketplace_service_item_pickup REAL,
    fd_item_services_marketplace_service_item_dropoff_pvz REAL,
    fd_item_services_marketplace_service_item_dropoff_sc REAL,
    fd_item_services_marketplace_service_item_dropoff_ff REAL,
    fd_item_services_marketplace_service_item_direct_flow_trans REAL,
    fd_item_services_marketplace_service_item_return_flow_trans REAL,
    fd_item_services_marketplace_service_item_deliv_to_customer REAL,
    fd_item_services_marketplace_service_item_return_not_deliv_to_customer REAL,
    fd_item_services_marketplace_service_item_return_part_goods_customer REAL,
    fd_item_services_marketplace_service_item_return_after_deliv_to_customer REAL
    );""".format(table_name3)

    print("CREATE TABLE sql_statement:\n", sql_statement)

    # call the function to create table
    execute_sql(cur, sql_statement, ident='table_name3')


def insertToTable(df, conn, cur, table):
    output = StringIO()
    df.to_csv(output, sep='\t', header=True, index=False)
    pprint(output)
    # df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)  # Required for rewinding the String object
    copy_query = f"COPY {table} FROM STDOUT csv DELIMITER '\t' NULL ''  ESCAPE '\\' HEADER "
    cur.copy_expert(copy_query, output)
    conn.commit()


def insertMPTable(conn, cur, df, tuples, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    # tuples = list(set([tuple(x) for x in df.to_numpy()]))

    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    names = ['order_uuid', 'order_id', 'account_list_id']
    # SQL query to execute
    query = sql.SQL("insert into mp_logs ({}) values ({})").format(
        # sql.Identifier(table),
        sql.SQL(', ').join(map(sql.Identifier, names)),
        sql.SQL(', ').join(sql.Placeholder() * len(names)))
    print(query.as_string(conn))
    # query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (
    #    table, cols)
    # query = "INSERT INTO cars (id, name, price) VALUES (%s, %s, %s)"

    # cur.executemany(query, tuples)

    try:
        cur.execute(query, tuples)
        # cur.executemany(query, tuples)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1


def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


    #
    #    def createTable(ps_conn, ps_sql):



# with conn:
#     try:
#         cur.execute('SELECT version()')
#
#         version = cur.fetchone()[0]
#         print(version)
#         cur.execute("DROP TABLE IF EXISTS cars")
#         cur.execute("CREATE TABLE cars(id SERIAL PRIMARY KEY, name VARCHAR(255), price INT)")
#         cur.execute("INSERT INTO cars(name, price) VALUES('Audi', 52642)")
#         cur.execute("INSERT INTO cars(name, price) VALUES('Mercedes', 57127)")
#         cur.execute("INSERT INTO cars(name, price) VALUES('Skoda', 9000)")
#         cur.execute("INSERT INTO cars(name, price) VALUES('Volvo', 29000)")
#
#     except psycopg2.DatabaseError as e:
#
#         print(f'Error {e}')
#         sys.exit(1)

#sys.exit(0)


def insertIntoTable(conn, cur, list_rec, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    # cols = ','.join(list(df.columns))
    # print(len(list(df.columns)), cols)
    # values = ','.join(['%%s'] * len(df.columns))
    # print(values)
    # que = f'INSERT INTO {table}({cols}) VALUES('
    # for x in df.to_numpy():
    for rec in list_rec:
        print(rec)
        cols = ', '.join(str(k) for k in rec)
        que = f'INSERT INTO {table}({cols}) VALUES('
        vals = tuple(val for val in rec.values())
        query = que + f'{vals})'
        print(query)
        print(len(rec), cols)
        print(len(vals), vals)
        try:
            cur.execute(query)
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)

    # tuples = list(set([tuple(x) for x in df.to_numpy()]))

    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    print(cols)
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s)" % (
        table, cols)

    try:
        cur.executemany(query, tuples)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1


def insertDF(conn, cur, df, table):

# df is the dataframe
    if len(df) > 0:
        df = df.fillna(psycopg2.extensions.AsIs('NULL'))
        df_columns = list(df)
        # create (col1,col2,...)
        columns = ",".join(df_columns)
    #
        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    #
        #create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table,columns, values)
    #
    #     cur = conn.cursor()
        execute_batch(cur, insert_stmt, df.values)
        conn.commit()
#     cur.close()


# import csv
# from io import StringIO
#
# from sqlalchemy import create_engine
#
# def psql_insert_copy(table, conn, keys, data_iter):
#     # gets a DBAPI connection that can provide a cursor
#     dbapi_conn = conn.connection
#     with dbapi_conn.cursor() as cur:
#         s_buf = StringIO()
#         writer = csv.writer(s_buf)
#         writer.writerows(data_iter)
#         s_buf.seek(0)
#
#         columns = ', '.join('"{}"'.format(k) for k in keys)
#         if table.schema:
#             table_name = '{}.{}'.format(table.schema, table.name)
#         else:
#             table_name = table.name
#
#         sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
#             table_name, columns)
#         cur.copy_expert(sql=sql, file=s_buf)
#
# engine = create_engine('postgresql://myusername:mypassword@myhost:5432/mydatabase')
# df.to_sql('table_name', engine, if_exists='replace', method=psql_insert_copy)
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import pandas as pd
import sys


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


def deleteTable(conn, tableName):

    cur = conn.cursor
    sql_statement = """
    DELETE FROM {} WHERE ctid IN 
    (SELECT ctid FROM (SELECT *, ctid, row_number() OVER (PARTITION BY order_id ORDER BY id DESC)
     FROM orders) s WHERE row_number >= 2)
    ;""".format(tableName)
    execute_sql(cur, sql_statement, ident=str(tableName))
    cur.close()


def dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    ps_conn = psycopg2.connect(**credentials)
    ps_conn.autocommit = True  # auto-commit each entry to the database
    #   ps_conn.cursor_factory = RealDictCursor
    ps_cur = ps_conn.cursor()
    print ("Connected Successfully to DB: " + str(db_parm) + "@" + str(host_parm))
    return ps_conn, ps_cur


def createTable(cur):

    table_name2 ='orders'
    sql_statement = """
    DROP TABLE IF EXISTS {}
    ;""".format(table_name2)
    execute_sql(cur, sql_statement, ident='table_name2')

    sql_statement = """
    CREATE TABLE {} (
    id SERIAL PRIMARY KEY NOT NULL,
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
    id BIGINT NOT NULL,
    order_id VARCHAR(50) NOT NULL,
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


def prepare_data_query(conn, table_orders, df_orders: pd.DataFrame, table_products, df_products: pd.DataFrame):

    df_orders = df_orders.fillna(psycopg2.extensions.AsIs('NULL'))
    column_data = ', '.join(df_orders.columns.to_list())
    s_data = '(' + ', '.join(['%s'] * len(df_orders.columns)) + ')'
    values_data = df_orders.to_numpy().tolist()
    list_val = []
    pointer = conn.cursor()
    for val_d in values_data:
        args = pointer.mogrify(s_data, tuple(val_d)).decode('utf8')
        insert_query = f"insert into {table_orders} ({column_data}) values {args} returning id, order_id"
        pointer.execute(insert_query)
        list_val.append(list(pointer.fetchone()[0:]))

    df = pd.DataFrame(list_val, columns=['id', 'order_id'])
    products = pd.merge(df_products, df, how='left', left_on='order_id', right_on='order_id')
    products = products.fillna(psycopg2.extensions.AsIs('NULL'))
    df_columns = list(products)
    columns = ",".join(df_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
    insert_stmt = f"INSERT INTO {table_products} ({columns}) {values}"
    execute_batch(pointer, insert_stmt, products.values)
    conn.commit()
    pointer.close()


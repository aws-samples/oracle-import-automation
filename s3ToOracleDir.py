from __future__ import print_function

import logging
import os
import boto3
import sys
import json
sys.path.append('lib')
import cx_Oracle as dbapi
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3Bucket = os.environ['S3BUCKET']
rdsDirectory = os.environ['RDSDIRECTORY']
dumpFileName = os.environ['DUMPFILENAME']
schemaName = os.environ['SCHEMANAME']
schemaPass = os.environ['SCHEMAPASSWORD']
tbsName = os.environ['TBSNAME']
fileName = "expdp"
hostName = os.environ['DB_HOSTNAME']
dbName = os.environ['DB_DATABASE']
logFileName = "impLog_" + hostName + "_" + dbName + "_" + schemaName + ".log"
# This is required for Oracle to generate an OID
# The  also requires an environment variable of
# HOSTALIASES=/tmp/HOSTALIASES
with open('/tmp/HOSTALIASES', 'w') as hosts_file:
    hosts_file.write('{} localhost\n'.format(os.uname()[1]))


def handler(event, context):

    logger.info('LD_LIBRARY_PATH: {}'.format(os.environ['LD_LIBRARY_PATH']))
    logger.info('ORACLE_HOME: {}'.format(os.environ['ORACLE_HOME']))

    # Connect to the database
    logger.info('Connecting to the database')
    db_connection = dbapi.connect('{user}/{password}@{host}:{port}/{database}'
                                  .format(user=os.environ['DB_USER'],
                                          password=os.environ['DB_PASSWORD'],
                                          host=os.environ['DB_HOSTNAME'],
                                          port=os.environ.get('DB_PORT', '1521'),
                                          database=os.environ['DB_DATABASE']))

    logger.info('Connecting to the database - success')
    cursor = db_connection.cursor()
    try:
                sql =  "SELECT rdsadmin.rdsadmin_s3_tasks.download_from_s3("
                sql += "p_bucket_name => '" + s3Bucket + "',"
                sql += "p_s3_prefix => '" + fileName + "',"
                sql += "p_directory_name => '" + rdsDirectory + "')"
                sql += " AS TASK_ID FROM DUAL"
                print(f"Running: {sql}")
                cursor.execute(sql)
                print(f"Waiting  05 seconds for dump file download ")
                time.sleep( 5 )
                sql_tbs = "create tablespace " + tbsName + " datafile size 1G autoextend on maxsize 15G"
                print(f"Running: {sql_tbs}")
                cursor.execute(sql_tbs)

                #sql_drop_usr = "drop user " + schemaName + " cascade"
                #print(f"Running: {sql_drop_usr}")
                #cursor.execute(sql_drop_usr)

                sql_user = "create user " + schemaName +" identified by " + schemaPass + " default tablespace " + tbsName
                print(f"Running: {sql_user}")
                cursor.execute(sql_user)

                sql_priv = "grant create session, resource to " + schemaName
                print(f"Running: {sql_priv}")
                cursor.execute(sql_priv)

                sql_quota = "alter user " + schemaName + " quota unlimited on " + tbsName
                print(f"Running: {sql_quota}")
                cursor.execute(sql_quota)

                sql_imp = "DECLARE "
                sql_imp += "v_hdnl NUMBER;"
                sql_imp += "BEGIN "
                sql_imp += "v_hdnl := DBMS_DATAPUMP.OPEN("
                sql_imp += "operation => \'IMPORT\',"
                sql_imp += "job_mode  => \'SCHEMA\',"
                sql_imp += "job_name  => null);"
                sql_imp += "DBMS_DATAPUMP.ADD_FILE("
                sql_imp += "handle    => v_hdnl,"
                sql_imp += "filename  => '" + dumpFileName +"',"
                sql_imp += "directory => '" + rdsDirectory +"',"
                sql_imp += "filetype  => dbms_datapump.ku$_file_type_dump_file);"
                sql_imp += "DBMS_DATAPUMP.ADD_FILE("
                sql_imp += "handle    => v_hdnl, "
                sql_imp += "filename  => '" + logFileName +"',"
                sql_imp += "directory => '" + rdsDirectory +"',"
                sql_imp += "filetype  => dbms_datapump.ku$_file_type_log_file);"
                sql_imp += "DBMS_DATAPUMP.METADATA_FILTER(v_hdnl,\'SCHEMA_EXPR\',\'IN (\'\'"+ schemaName +"\'\')\');"
                sql_imp += "DBMS_DATAPUMP.START_JOB(v_hdnl);"
                sql_imp += "END;"
                print(f"Running: {sql_imp}")
                cursor.execute(sql_imp)


    except dbapi.DatabaseError as e:
        logging.error('Database error: {}'.format(str(e)))
        raise e
    finally:
        db_connection.close()

    return {
        'result': 'success',
        'cx_Oracle.version': dbapi.version
    }

if __name__ == "__main__":
    print(handler({}, {})

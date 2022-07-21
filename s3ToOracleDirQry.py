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

                sqlupd =  "SELECT rdsadmin.rdsadmin_s3_tasks.upload_to_s3("
                sqlupd += "p_bucket_name => '" + s3Bucket + "',"
                sqlupd += "p_prefix => '" + logFileName + "',"
                sqlupd += "p_s3_prefix => '',"
                sqlupd += "p_directory_name => '" + rdsDirectory + "')"
                sqlupd += " AS TASK_ID FROM DUAL"
                print(f"Running: {sqlupd}")
                cursor.execute(sqlupd)

                op = "select object_type,count(*) from dba_objects where "
                op += "owner = '"+ schemaName +"' "
                op += "group by object_type"
                print(f"Running: {op}")
                cursor.execute(op)

                while True:
                    rows = cursor.fetchmany(100)
                    if not rows:
                        break
                    for row in rows:
                        print(f"Output: {row}")

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
    print(handler({}, {}))

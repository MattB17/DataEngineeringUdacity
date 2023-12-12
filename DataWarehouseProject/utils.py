import boto3
import configparser
import psycopg2


def get_config(config_file):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config


def get_iam_client(config):
    return boto3.client('iam',
                        region_name="us-west-2",
                        aws_access_key_id=config.get('AWS', 'key'),
                        aws_secret_access_key=config.get('AWS', 'secret'))


def get_redshift_client(config):
    return boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=config.get('AWS', 'key'),
                        aws_secret_access_key=config.get('AWS', 'secret'))


def get_ec2_resource(config):
    return boto3.resource('ec2',
                          region_name="us-west-2",
                          aws_access_key_id=config.get('AWS', 'key'),
                          aws_secret_access_key=config.get('AWS', 'secret'))

def connect_to_database(config):
    try:
        conn = psycopg2.connect(
          "host={0} dbname={1} user={2} password={3} port={4}".format(
            config.get('CLUSTER', 'endpoint'),
            config.get('CLUSTER', 'db_name'),
            config.get('CLUSTER', 'db_user'),
            config.get('CLUSTER', 'db_password'),
            config.get('CLUSTER', 'db_port')))
        return conn
    except Exception as e:
        print(e)

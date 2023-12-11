import boto3
import configparser

def get_config(config_file):
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config

def get_iam_client(config):
    iam_client = boto3.client('iam',
                              region_name="us-west-2",
                              aws_access_key_id=config.get('AWS', 'key'),
                              aws_secret_access_key=config.get('AWS', 'secret'))
    return iam_client

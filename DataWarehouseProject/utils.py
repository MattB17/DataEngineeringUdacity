"""
A collection of utility functions used throughout the Sparkify data pipeline.

"""
import boto3
import configparser
import psycopg2


def get_config(config_file):
    """
    Loads configuration parameters from `config_file`.

    Parameters
    ----------
    config_file: A string representing the path to a configuration file.

    Returns
    -------
    The set of configuration parameters loaded from `config_file`.

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config


def get_iam_client(config):
    """
    Retrieves the AWS IAM client based on `config`.

    Parameters
    ----------
    config: The set of configuration parameters used to create the AWS IAM
            client.

    Returns
    -------
    The IAM client that can be used to access AWS IAM.

    """
    return boto3.client('iam',
                        region_name=config.get('AWS', 'region'),
                        aws_access_key_id=config.get('AWS', 'key'),
                        aws_secret_access_key=config.get('AWS', 'secret'))


def get_redshift_client(config):
    """
    Retrieves the AWS Redshift client based on `config`.

    Parameters
    ----------
    config: The set of configuration parameters used to create the AWS Redshift
            client.

    Returns
    -------
    The Redshift client that can used to access AWS Redshift.

    """
    return boto3.client('redshift',
                        region_name=config.get('AWS', 'region'),
                        aws_access_key_id=config.get('AWS', 'key'),
                        aws_secret_access_key=config.get('AWS', 'secret'))


def get_ec2_resource(config):
    """
    Retrieves the AWS EC2 resource based on `config`.

    Parameters
    ----------
    config: The set of configuration parameters used to create the AWS EC2
            resource.

    Returns
    -------
    The EC2 resource that can be used to access AWS EC2.

    """
    return boto3.resource('ec2',
                          region_name=config.get('AWS', 'region'),
                          aws_access_key_id=config.get('AWS', 'key'),
                          aws_secret_access_key=config.get('AWS', 'secret'))

def connect_to_database(config):
    """
    Connects to the Redshift cluster specified in `config`.

    Parameters
    ----------
    config: The set of configuration parameters used to connect to the database.

    Returns
    -------
    None

    """
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

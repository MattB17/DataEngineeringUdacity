"""
An IaC script to setup an IAM role and redshift cluster.

"""

import pandas as pd
import json
import time
import utils

def create_iam_redshift_role(iam_client, role_name):
    """
    Creates an IAM role to allow Redshift clusters to call AWS services.

    Parameters
    ----------
    iam_client: The iam client used to interact with AWS IAM. Obtained by
                calling `boto3.client('iam', ...)`.
    role_name: The name of the Redshift role being created.

    Returns
    -------
    None

    """
    try:
        response = iam_client.create_role(
          Path='/',
          RoleName=role_name,
          Description="Allows Redshift clusters to call AWS services.",
          AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                            'Effect': 'Allow',
                            'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'}
          )
        )
    except Exception as e:
        print(e)


def create_redshift_cluster(redshift_client, config):
    """
    Creates a Redshift cluster based on the settings in `config`.

    Parameters
    ----------
    redshift_client: The Redshift client used to interact with AWS. Obtained
                     from calling `boto3.client('redshift', ...)`.
    config: The set of configuration parameters used in cluster creation.

    Returns
    -------
    None

    """
    try:
        response = redshift_client.create_cluster(
          # Hardware parameters.
          ClusterType=config.get('CLUSTER', 'cluster_type'),
          NodeType=config.get('CLUSTER', 'node_type'),
          NumberOfNodes=int(config.get('CLUSTER', 'num_nodes')),

          # Identifiers & Credentials.
          DBName=config.get('CLUSTER', 'db_name'),
          ClusterIdentifier=config.get('CLUSTER', 'cluster_identifier'),
          MasterUsername=config.get('CLUSTER', 'db_user'),
          MasterUserPassword=config.get('CLUSTER', 'db_password'),

          # IAM role to allow S3 access.
          IamRoles=[config.get('IAM_ROLE', 'role_arn')])
    except Exception as e:
        print(e)


def get_cluster_properties(redshift_client, cluster_identifier):
    """
    Retrieves the Redshift cluster properties of `cluster_identifier`.

    Parameters
    ----------
    redshift_client: The Redshift client used to interact with AWS. Obtained
                     from calling `boto3.client('redshift', ...)`.
    cluster_identifier: A string identifying the cluster for which the
                        properties are retrieved.

    Returns
    -------
    The set properties corresponding the the Redshift cluster associated with
    `cluster_identifier`.

    """
    return redshift_client.describe_clusters(
      ClusterIdentifier=cluster_identifier)['Clusters'][0]


def setup_tcp_port_for_cluster(ec2_resource, cluster_vpc_id, port):
    """
    Sets up the ingress TCP port for the Redshift cluster to accept connections.

    Parameters
    ----------
    ec2_resource: The EC2 resource used to interact with AWS EC2.
    cluster_vpc_id: The identifier for the cluster's VPC.
    port: The port on which client's connect and receive data from the Redshift
          cluster.

    Returns
    -------
    None

    """
    try:
        vpc = ec2_resource.Vpc(id=cluster_vpc_id)
        default_sg = list(vpc.security_groups.all())[0]

        default_sg.authorize_ingress(
          GroupName=default_sg.group_name,
          CidrIp='0.0.0.0/0',
          IpProtocol='TCP',
          FromPort=port,
          ToPort=port)
    except Exception as e:
        print(e)


def setup_iam_role_for_redshift(config):
    """
    Sets up the IAM role allowing Redshift to read from AWS S3.

    Parameters
    ----------
    config: A set of configuration parameters used to setup the IAM role.

    Returns
    -------
    A string representing the ARN for the created IAM role.

    """
    iam_client = utils.get_iam_client(config)

    role_name = config.get('IAM_ROLE', 'iam_role_name')

    # Create the IAM role.
    create_iam_redshift_role(iam_client, role_name)

    # Attach the S3 read access to the role.
    iam_client.attach_role_policy(
      RoleName=role_name,
      PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    return iam_client.get_role(RoleName=role_name)['Role']['Arn']


def setup_redshift_cluster(config):
    """
    Sets up the Redshift cluster based on `config`.

    Parameters
    ----------
    config: A set of configuration parameters used to setup the Redshift
            cluster.

    Returns
    -------
    A string representing the endpoint for the created Redshift cluster.

    """
    redshift_client = utils.get_redshift_client(config)

    create_redshift_cluster(redshift_client, config)

    cluster_identifier = config.get('CLUSTER', 'cluster_identifier')
    cluster_properties = get_cluster_properties(
      redshift_client, cluster_identifier)

    while cluster_properties['ClusterStatus'].lower() != "available":
        # Sleep for 30 seconds then refresh the cluster properties.
        print("Status: {}".format(cluster_properties['ClusterStatus']))
        time.sleep(30)
        cluster_properties = get_cluster_properties(
          redshift_client, cluster_identifier)

    ec2_resource = utils.get_ec2_resource(config)
    setup_tcp_port_for_cluster(
      ec2_resource, cluster_properties['VpcId'],
      int(config.get('CLUSTER', 'db_port')))

    endpoint = cluster_properties['Endpoint']['Address']
    return endpoint


def main():
    config = utils.get_config('dwh.cfg')

    try:
        config['IAM_ROLE']['role_arn'] = setup_iam_role_for_redshift(config)
    except Exception as e:
        print(e)

    try:
        endpoint = setup_redshift_cluster(config)
        config['CLUSTER']['endpoint'] = endpoint
    except Exception as e:
        print(e)

    # Test database connection.
    conn = utils.connect_to_database(config)
    cur = conn.cursor()

    print("Connected Successfully")

    # Writes the config back to the file with the addition of the IAM role ARN
    # and the Redshift cluster endpoint.
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)


if __name__ == '__main__':
    main()

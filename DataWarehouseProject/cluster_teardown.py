"""
An IaC script to delete the Redshift cluster and IAM role used in the pipeline
for the Sparkify data warehouse.

"""

import utils
import time

def get_cluster_status(redshift_client, config):
    """
    A string representing the status of the cluster.

    Used for polling for cluster status. If the cluster status cannot be
    be retrieved this means the cluster is not available and has thus been
    deleted. In this case, a string signifying this scenario is returned.

    Parameters
    ----------
    redshift_client: The Redshift client used to access AWS Redshift. Obtained
                     from calling `boto3.client('redshift', ...)`.
    config: The set of configuration parameters identifying the cluster.

    Returns
    -------
    A string representing the status of the cluster.

    """
    try:
        status = redshift_client.describe_clusters(
          ClusterIdentifier=config.get('CLUSTER', 'cluster_identifier'))
        return status['Clusters'][0]['ClusterStatus'].lower()
    except Exception as e:
        return "deleted"


def delete_cluster(config):
    """
    Deletes the Redshift cluster specified by `config`.

    Parameters
    ----------
    config: The set of configuration parameters specifying the cluster to be
            deleted.

    Returns
    -------
    None

    """
    redshift_client = utils.get_redshift_client(config)
    redshift_client.delete_cluster(
      ClusterIdentifier=config.get('CLUSTER', 'cluster_identifier'),
      SkipFinalClusterSnapshot=True)

    # We continually poll for cluster status until the cluster is deleted.
    status = get_cluster_status(redshift_client, config)
    while status != "deleted":
        # Sleep for 30 seconds then refresh the cluster status.
        print("Status: {}".format(status))
        time.sleep(30)
        status = get_cluster_status(redshift_client, config)



def remove_iam_role(config):
    """
    Removes the IAM role used with the redshift cluster.

    Parameters
    ----------
    config: A set of configuration parameters specifying the IAM role to be
            deleted.

    Returns
    -------
    None

    """
    iam_client = utils.get_iam_client(config)
    role_name = config.get('IAM_ROLE', 'iam_role_name')

    iam_client.detach_role_policy(
      RoleName=role_name,
      PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam_client.delete_role(RoleName=role_name)


def main():
    config = utils.get_config('dwh.cfg')

    delete_cluster(config)
    remove_iam_role(config)


if __name__ == "__main__":
    main()

import utils
import time

def get_cluster_status(redshift_client, config):
    try:
        status = redshift_client.describe_clusters(
          ClusterIdentifier=config.get('CLUSTER', 'cluster_identifier'))
        return status['Clusters'][0]['ClusterStatus'].lower()
    except Exception as e:
        return "deleted"


def delete_cluster(config):
    redshift_client = utils.get_redshift_client(config)
    redshift_client.delete_cluster(
      ClusterIdentifier=config.get('CLUSTER', 'cluster_identifier'),
      SkipFinalClusterSnapshot=True)

    status = get_cluster_status(redshift_client, config)
    while status != "deleted":
        # Sleep for 30 seconds then refresh the cluster status.
        print("Status: {}".format(status))
        time.sleep(30)
        status = get_cluster_status(redshift_client, config)



def remove_iam_role(config):
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

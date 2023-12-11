import utils


def delete_cluster(config):
    redshift_client = utils.get_redshift_client(config)
    redshift_client.delete_cluster(
      ClusterIdentifier=config.get('CLUSTER', 'cluster_identifier'),
      SkipFinalClusterSnapshot=True)


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

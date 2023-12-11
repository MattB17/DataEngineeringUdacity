import pandas as pd
import json
import utils

def create_iam_redshift_role(iam_client, role_name):
    try:
        iam_client.create_role(
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

def setup_iam_role_for_redshift(config):
    iam_client = utils.get_iam_client(config)

    role_name = config.get('IAM_ROLE', 'iam_role_name')

    # Create the IAM role.
    create_iam_redshift_role(iam_client, role_name)

    # Attach the S3 read access to the role.
    iam_client.attach_role_policy(
      RoleName=role_name,
      PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    return iam_client.get_role(RoleName=role_name)['Role']['Arn']

def main():
    config = utils.get_config('dwh.cfg')

    config['IAM_ROLE']['role_arn'] = setup_iam_role_for_redshift(config)

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)


if __name__ == '__main__':
    main()

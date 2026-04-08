import boto3
import os


def handler(event, context):
    ec2 = boto3.client("ec2", region_name=os.environ["DEPLOY_REGION"])

    response = ec2.run_instances(
        LaunchTemplate={
            "LaunchTemplateName": os.environ["LAUNCH_TEMPLATE_NAME"],
            "Version": "$Latest",
        },
        MinCount=1,
        MaxCount=1,
    )

    instance_id = response["Instances"][0]["InstanceId"]
    print(f"Started instance: {instance_id}")

    return {"statusCode": 200, "body": f"Started {instance_id}"}

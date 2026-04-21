import json
import boto3
import urllib.parse
import logging

# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

stepfunctions = boto3.client('stepfunctions')

STATE_MACHINE_ARN = "arn:aws:states:eu-north-1:975050150070:stateMachine:Project-first-aws-08apr2025"

def lambda_handler(event, context):
    try:
        logger.info("Lambda triggered")
        logger.info(f"Incoming event: {json.dumps(event)}")

        record = event['Records'][0]

        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        logger.info(f"File uploaded: s3://{bucket}/{key}")

        # ONLY trigger when control file in RAW folder is uploaded
        if key != "raw/_ready.json":
            logger.info("Not control file. Ignoring event.")
            return {
                'statusCode': 200,
                'body': json.dumps('Ignored non-trigger file')
            }

        logger.info("Control file detected. Starting Step Function...")

        step_input = {
            "bucket": bucket,
            "key": key
        }

        response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(step_input)
        )

        logger.info(f"Step Function started: {response['executionArn']}")

        return {
            'statusCode': 200,
            'body': json.dumps('Pipeline triggered successfully!')
        }

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        raise e

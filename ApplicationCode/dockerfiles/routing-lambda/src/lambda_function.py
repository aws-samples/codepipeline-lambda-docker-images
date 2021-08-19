#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

import os
import json
import logging
import uuid
import decimal
from urllib.parse import unquote_plus

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sqs = boto3.resource('sqs')
dynamodb = boto3.resource("dynamodb")
dataset_table = dynamodb.Table('octagon-Datasets-{}'.format(os.environ['ENV']))


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def get_item(table, team, dataset):
    try:
        response = table.get_item(
            Key={
                'name': '{}-{}'.format(team, dataset)
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        item = response['Item']
        return item['pipeline']


def parse_s3_event(s3_event):
    return {
        'bucket': s3_event['s3']['bucket']['name'],
        'key': unquote_plus(s3_event['s3']['object']['key']),
        'stage': 'raw',
        'size': s3_event['s3']['object']['size'],
        'landing_time': s3_event['eventTime']
    }


def lambda_handler(event, context):
    try:
        print(json.dumps(event))
        logger.info('Received {} messages'.format(len(event['Records'])))
        for record in event['Records']:
            logger.info('Parsing S3 Event')
            message = parse_s3_event(json.loads(record['body'])['Records'][0])

            if os.environ['NUM_BUCKETS'] == '1':
                team = message['key'].split('/')[1]
                dataset = message['key'].split('/')[2]
            elif len(message['key'].split('/')) == 5:
                team = message['key'].split('/')[0]
                dataset = message['key'].split(
                    '/')[1] + '-' + message['key'].split('/')[2]
                partition = message['key'].split('/')[-2]
            else:
                team = message['key'].split('/')[0]
                dataset = message['key'].split('/')[1]
                partition = message['key'].split('/')[-2]
            message['team'] = team
            message['dataset'] = dataset
            pipeline = get_item(dataset_table, team, dataset)
            message['pipeline'] = pipeline
            message['partition'] = partition
            runtime_region = os.environ['AWS_REGION']
            print(runtime_region)
            logger.info(
                'Sending event to {}-{} pipeline queue for processing'.format(team, pipeline))
            a = '{}-{}-{}-{}-{}-{}-queue-a.fifo'.format(
                'sdlf',
                team,
                pipeline,
                os.environ['ORG'],
                os.environ['APP'],
                os.environ['ENV']
                # team,
                # pipeline
            )
            print(a)
            queue = sqs.get_queue_by_name(QueueName='{}-{}-{}-{}-{}-{}-queue-a.fifo'.format(
                'sdlf',
                team,
                pipeline,
                os.environ['ORG'],
                os.environ['APP'],
                os.environ['ENV']
                # team,
                # pipeline
            ))
            print(queue)
            runtime_region = os.environ['AWS_REGION']
            print(runtime_region)
            queue.send_message(MessageBody=json.dumps(
                message), MessageGroupId='{}-{}'.format(team, dataset), MessageDeduplicationId=str(uuid.uuid1()))
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return

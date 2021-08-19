#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

import os

import boto3

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration, SQSConfiguration,\
    StateMachineConfiguration, S3Configuration
from datalake_library.interfaces.dynamo_interface import DynamoInterface
from datalake_library.interfaces.sqs_interface import SQSInterface
from datalake_library.interfaces.states_interface import StatesInterface

logger = init_logger(__name__)


def lambda_handler(event, context):
    """Checks if any items need processing and triggers state machine
    Arguments:
        event {dict} -- Dictionary with no relevant details
        context {dict} -- Dictionary with details on Lambda context 
    """
    
    # TODO Implement Redrive Logic (through message_group_id)
    try:
        team = event['team']
        pipeline = event['pipeline']
        dataset = event['dataset']
        stage_bucket = S3Configuration().stage_bucket
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)
        transform_info = dynamo_interface.get_transform_table_item('{}-{}'.format(team, dataset)) 
        MIN_ITEMS_TO_PROCESS = int(transform_info['min_items_process']) 
        MAX_ITEMS_TO_PROCESS = int(transform_info['max_items_process']) 
        sqs_config = SQSConfiguration(team, pipeline, dataset)
        queue_interface = SQSInterface(sqs_config.get_post_stage_queue_name)
        keys_to_process = []
        
        logger.info('Querying {}-{} objects waiting for processing'.format(team, dataset))
        keys_to_process = queue_interface.receive_min_max_messages(MIN_ITEMS_TO_PROCESS, MAX_ITEMS_TO_PROCESS)
        # If no keys to process, break    
        if not keys_to_process:
            return

        logger.info('{} Objects ready for processing'.format(len(keys_to_process)))
        keys_to_process = list(set(keys_to_process))

        response = {
            'statusCode': 200,
            'body': {
                    "bucket": stage_bucket,
                    "keysToProcess": keys_to_process,
                    "team": team,
                    "pipeline": pipeline,                    
                    "dataset": dataset
                    }
        }
        logger.info('Starting State Machine Execution')
        state_config = StateMachineConfiguration(team, pipeline)
        StatesInterface().run_state_machine(state_config.get_post_stage_state_machine_arn, response)
    except Exception as e:
        # If failure send to DLQ
        if keys_to_process:
            dlq_interface = SQSInterface(sqs_config.get_post_stage_dlq_name)
            for key in keys_to_process:
                dlq_interface.send_message_to_fifo_queue(key, 'failed')
        logger.error("Fatal error", exc_info=True)
        raise e
    return

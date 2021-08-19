#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration, SQSConfiguration, S3Configuration
from datalake_library.interfaces.dynamo_interface import DynamoInterface
from datalake_library.interfaces.sqs_interface import SQSInterface
from datalake_library import octagon
from datalake_library.octagon import Artifact, EventReasonEnum, peh

logger = init_logger(__name__)
octagon_client = (
    octagon.OctagonClient()
    .with_run_lambda(True)
    .build()
)


def lambda_handler(event, context):
    """Updates the S3 objects metadata catalog
    
    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context
    
    Returns:
        {dict} -- Dictionary with outcome of the process
    """
    try:
        component = context.function_name.split('-')[-2].title()
        peh.PipelineExecutionHistoryAPI(octagon_client).retrieve_pipeline_execution(event['body']['peh_id'])
        octagon_client.update_pipeline_execution(status="Pre-Stage {} Processing".format(component), component=component)

        logger.info('Fetching transformed objects')
        processed_keys = event['body']['processedKeys']
        team = event['body']['team']
        pipeline = event['body']['pipeline']
        dataset = event['body']['dataset']

        logger.info('Initializing DynamoDB config and Interface')
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)

        logger.info('Storing metadata to DynamoDB')
        for key in processed_keys:
            object_metadata = {
                'bucket': S3Configuration().stage_bucket,
                'key': key,
                'team': team,
                'pipeline': pipeline,
                'dataset': dataset,
                'stage': 'pre-stage'
            }
            
            dynamo_interface.update_object_metadata_catalog(object_metadata)

        logger.info('Sending messages to next SQS queue if it exists')
        sqs_config = SQSConfiguration(team, pipeline, dataset)
        sqs_interface = SQSInterface(sqs_config.get_post_stage_queue_name)
        sqs_interface.send_batch_messages_to_fifo_queue(processed_keys, 10, '{}-{}'.format(team, dataset))

        octagon_client.end_pipeline_execution_success()
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="Pre-Stage {} Error: {}".format(component, repr(e)))
        raise e
    return 200

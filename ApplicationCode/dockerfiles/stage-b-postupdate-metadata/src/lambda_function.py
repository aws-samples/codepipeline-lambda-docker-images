#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration, SQSConfiguration
from datalake_library.interfaces.dynamo_interface import DynamoInterface
from datalake_library.interfaces.s3_interface import S3Interface
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
        event {dict} -- Dictionary with details on Bucket and Keys
        context {dict} -- Dictionary with details on Lambda context
    
    Returns:
        {dict} -- Dictionary with response
    """
    try:
        component = context.function_name.split('-')[-2].title()
        peh.PipelineExecutionHistoryAPI(octagon_client).retrieve_pipeline_execution(event['body']['job']['peh_id'])
        octagon_client.update_pipeline_execution(status="Post-Stage {} Processing".format(component), component=component)

        bucket = event['body']['bucket']
        processed_keys_path = event['body']['job']['processedKeysPath']
        processed_keys = S3Interface().list_objects(bucket, processed_keys_path)
        team = event['body']['team']
        pipeline = event['body']['pipeline']
        dataset = event['body']['dataset']
        
        logger.info('Initializing DynamoDB config and Interface')
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)

        logger.info('Storing metadata to DynamoDB')
        for key in processed_keys:
            object_metadata = {
                'bucket': bucket,
                'key': key,
                'team': team,
                'pipeline': pipeline,
                'dataset': dataset,
                'stage': 'post-stage'
            }
            dynamo_interface.update_object_metadata_catalog(object_metadata)
        
        octagon_client.end_pipeline_execution_success()
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="Post-Stage {} Error: {}".format(component, repr(e)))
        raise e
    return 200

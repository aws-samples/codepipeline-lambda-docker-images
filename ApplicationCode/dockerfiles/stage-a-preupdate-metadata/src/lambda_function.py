#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

import json

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration
from datalake_library.interfaces.dynamo_interface import DynamoInterface
from datalake_library import octagon
from datalake_library.octagon import Artifact, EventReasonEnum, peh

logger = init_logger(__name__)
octagon_client = (
    octagon.OctagonClient()
    .with_run_lambda(True)
    .build()
)


def lambda_handler(event, context):
    """Updates the objects metadata catalog
    
    Arguments:
        event {dict} -- Dictionary with details on S3 event
        context {dict} -- Dictionary with details on Lambda context
    
    Returns:
        {dict} -- Dictionary with Processed Bucket and Key
    """
    try:
        logger.info('Building object metadata from S3 write event')
        component = context.function_name.split('-')[-2].title()
        object_metadata = json.loads(event)
        object_metadata['peh_id'] = octagon_client.start_pipeline_execution(pipeline_name='{}-{}-pre-stage'.format(object_metadata['team'], object_metadata['pipeline']), 
                                                                            comment=event)
        octagon_client.update_pipeline_execution(status="Pre-Stage {} Processing".format(component), component=component)
        # Add business metadata (e.g. object_metadata['project'] = 'xyz')

        logger.info('Initializing DynamoDB config and Interface')
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)
        
        logger.info('Storing metadata to DynamoDB')
        dynamo_interface.update_object_metadata_catalog(object_metadata)
        
        logger.info('Passing arguments to the next function of the state machine')
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="Pre-Stage {} Error: {}".format(component, repr(e)))
        raise e
    return {
        'statusCode': 200,
        'body': object_metadata
    }

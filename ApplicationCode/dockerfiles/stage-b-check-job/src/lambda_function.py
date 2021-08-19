#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

from datalake_library.commons import init_logger
from datalake_library.transforms.transform_handler import TransformHandler
from datalake_library import octagon
from datalake_library.octagon import Artifact, EventReasonEnum, peh

logger = init_logger(__name__)
octagon_client = (
    octagon.OctagonClient()
    .with_run_lambda(True)
    .build()
)


def lambda_handler(event, context):
    """Calls custom job waiter developed by user
    
    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context
    
    Returns:
        {dict} -- Dictionary with Processed Bucket, Key(s) and Job Details
    """
    try:
        logger.info('Stage B Check Job Status')

        logger.info('Fetching bucket and key from previous step')
        bucket = event['body']['bucket']
        keys_to_process = event['body']['keysToProcess']
        team = event['body']['team']
        dataset = event['body']['dataset']
        job_details = event['body']['job']['jobDetails']
        processed_keys_path = event['body']['job']['processedKeysPath']
        
        logger.info('Checking Job Status')
        response = TransformHandler().stage_b_job_status(bucket, keys_to_process, team, dataset, processed_keys_path, job_details)
        response['peh_id'] = event['body']['job']['peh_id']
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        component = context.function_name.split('-')[-2].title()
        peh.PipelineExecutionHistoryAPI(octagon_client).retrieve_pipeline_execution(event['body']['job']['peh_id'])
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="Post-Stage {} Error: {}".format(component, repr(e)))
        raise e
    return response

#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

import os
import shutil

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


def remove_content_tmp():
    ## Remove contents of the Lambda /tmp folder (Not released by default)
    for root, dirs, files in os.walk('/tmp'):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))

def lambda_handler(event, context):
    """Calls custom transform developed by user
    
    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context
    
    Returns:
        {dict} -- Dictionary with Processed Bucket and Key(s)
    """ 
    try:
        logger.info('Stage B Transformation Lambda')
        component = context.function_name.split('-')[-2].title()
        peh_id = octagon_client.start_pipeline_execution(pipeline_name='{}-{}-post-stage'.format(event['body']['team'], event['body']['pipeline']), comment=event)
        octagon_client.update_pipeline_execution(status="Post-Stage {} Processing".format(component), component=component)

        logger.info('Fetching bucket and key(s) from previous step')
        bucket = event['body']['bucket']
        keys_to_process = event['body']['keysToProcess']
        team= event['body']['team']
        dataset = event['body']['dataset']
        
        ## Call custom transform created by user and process the file
        logger.info('Custom Processing Objects')
        response = TransformHandler().stage_b_transform(bucket, keys_to_process, team, dataset)
        response['peh_id'] = peh_id
        remove_content_tmp()
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="Post-Stage {} Error: {}".format(component, repr(e)))
        remove_content_tmp()
        raise e
    return response

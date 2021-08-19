#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

import json

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import SQSConfiguration
from datalake_library.interfaces.sqs_interface import SQSInterface

logger = init_logger(__name__)

    
def lambda_handler(event, context):
    try:
        if isinstance(event, str):
            event = json.loads(event)
        sqs_config = SQSConfiguration(event['body']['team'], event['body']['pipeline'], event['body']['dataset'])
        sqs_interface = SQSInterface(sqs_config.get_post_stage_dlq_name)
        
        logger.info('Execution Failed. Sending original payload to DLQ')
        sqs_interface.send_message_to_fifo_queue(json.dumps(event), 'failed')
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return

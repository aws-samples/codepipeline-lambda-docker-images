#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

import json

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import StateMachineConfiguration
from datalake_library.interfaces.states_interface import StatesInterface

logger = init_logger(__name__)


def lambda_handler(event, context):
    try:
        logger.info('Received {} messages'.format(len(event['Records'])))
        for record in event['Records']:
            logger.info('Starting State Machine Execution')
            state_config = StateMachineConfiguration(json.loads(record['body'])['team'],
                                                     json.loads(record['body'])['pipeline'])
            StatesInterface().run_state_machine(state_config.get_pre_stage_state_machine_arn, record['body'])
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        raise e
    return

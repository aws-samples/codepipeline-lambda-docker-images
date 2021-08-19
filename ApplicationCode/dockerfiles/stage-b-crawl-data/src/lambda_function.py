#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

import boto3

from datalake_library.commons import init_logger
from datalake_library import octagon
from datalake_library.octagon import Artifact, EventReasonEnum, peh

logger = init_logger(__name__)
octagon_client = (
    octagon.OctagonClient()
    .with_run_lambda(True)
    .build()
)

client = boto3.client('glue')


def lambda_handler(event, context):
    """Crawl Data using specified Glue Crawler

    Arguments:
        event {dict} -- Dictionary with details on Bucket and Keys
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket and Keys Path
    """
    try:
        component = context.function_name.split('-')[-2].title()
        peh.PipelineExecutionHistoryAPI(octagon_client).retrieve_pipeline_execution(
            event['body']['job']['peh_id'])
        octagon_client.update_pipeline_execution(
            status="Post-Stage {} Processing".format(component), component=component)

        team = event['body']['team']
        dataset = event['body']['dataset']

        crawler_name = '-'.join(['sdlf', team, dataset, 'post-stage-crawler'])
        logger.info('Starting Crawler {}'.format(crawler_name))
        try:
            client.start_crawler(Name=crawler_name)
        except client.exceptions.CrawlerRunningException:
            logger.info('Crawler is already running')
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        octagon_client.end_pipeline_execution_failed(component=component,
                                                     issue_comment="Post-Stage {} Error: {}".format(component, repr(e)))
        raise e
    return 200

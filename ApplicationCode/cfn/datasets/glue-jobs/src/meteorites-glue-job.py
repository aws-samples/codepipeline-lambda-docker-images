#  Copyright Amazon.com, Inc. and its affiliates. All Rights Reserved.
#  SPDX-License-Identifier: MIT
#  
#  Licensed under the MIT License. See the LICENSE accompanying this file
#  for the specific language governing permissions and limitations under
#  the License.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_LOCATION', 'OUTPUT_LOCATION'])
source = args['SOURCE_LOCATION']
destination = args['OUTPUT_LOCATION']

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dyf = glueContext.create_dynamic_frame.from_options(
	    connection_type="s3",
	    format="csv",
	    connection_options={
	        "paths": [source]
	    },
	    format_options={
	        "withHeader": False,
	        "separator": ","
		},
		transformation_ctx="path={}".format(source)
	)

dyf_tmp = dyf.apply_mapping([('col0', 'string', 'id', 'long'), 
                             ('col1', 'string', 'name', 'string'),
                             ('col2', 'string', 'cls', 'string'),
                             ('col3', 'string', 'lon', 'double'),
                             ('col4', 'string', 'lat', 'double')],
							 transformation_ctx = "applymapping")

# Write it out in Parquet
glueContext.write_dynamic_frame.from_options(frame = dyf_tmp, connection_type = "s3",
											 connection_options = {"path": destination}, format = "parquet",
											 transformation_ctx = "path={}".format(destination))

job.commit()

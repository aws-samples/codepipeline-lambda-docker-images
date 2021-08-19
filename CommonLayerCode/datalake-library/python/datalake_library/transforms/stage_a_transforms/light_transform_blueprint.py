#######################################################
## Blueprint example of a custom transformation
## where a JSON file is dowloaded from RAW to /tmp
## then transformed to CSV before being re-uploaded to STAGE 
#######################################################
## License: Apache 2.0
#######################################################
## Author: jaidi
#######################################################

#######################################################
## Import section
## common-pipLibrary repository can be leveraged
## to add external libraries as a layer if need be
#######################################################
import json

#######################################################
## Use S3 Interface to interact with S3 objects
## For example to download/upload them
#######################################################
from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import S3Configuration
from datalake_library.interfaces.s3_interface import S3Interface

s3_interface = S3Interface()
# IMPORTANT: Stage bucket where transformed data must be uploaded
stage_bucket = S3Configuration().stage_bucket

logger = init_logger(__name__)


class CustomTransform():
    def __init__(self):
        logger.info("S3 Blueprint Light Transform initiated")
        
    def transform_object(self, bucket, key, team, dataset):
        # Download S3 object locally to /tmp directory
        # The s3_helper.download_object method
        # returns the local path where the file was saved
        local_path = s3_interface.download_object(bucket, key)
        
        # Apply business business logic:
        # Below example is opening a JSON file and
        # extracting fields, then saving the file to 
        # CSV locally and re-uploading to Stage bucket

        # Reading file locally
        with open(local_path, 'r') as raw:
                data = raw.read()
        
        json_data = json.loads(data)
        
        # Saving file locally as CSV to /tmp after extracting fields of interest
        output_path = "{}.csv".format(local_path.split('.')[0])
        with open(output_path, "w") as write_file:
            write_file.write('{}, {}, {}, {}, {}'.format(
                json_data['id'], json_data['name'], json_data['recclass'],
                json_data['reclong'], json_data['reclat'])
            )

        # Uploading file to Stage bucket at appropriate path
        # IMPORTANT: Build the output s3_path without the s3://stage-bucket/
        s3_path = 'pre-stage/{}/{}/{}'.format(team, dataset, output_path.split('/')[2])
        # IMPORTANT: Notice "stage_bucket" not "bucket"
        s3_interface.upload_object(output_path, stage_bucket, s3_path)
        # IMPORTANT S3 path(s) must be stored in a list
        processed_keys = [s3_path]

        #######################################################
        ## IMPORTANT
        ## This function must return a Python list
        ## of transformed S3 paths. Example: 
        ## ['pre-stage/engineering/meteorites/landing1.csv','pre-stage/engineering/meteorites/landing2.csv']
        #######################################################

        return processed_keys

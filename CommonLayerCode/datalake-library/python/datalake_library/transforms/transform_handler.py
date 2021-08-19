from importlib import import_module

from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration
from datalake_library.interfaces.dynamo_interface import DynamoInterface

logger = init_logger(__name__)

 
class TransformHandler: 
    def __init__(self): 
        logger.info("Transformation Handler initiated") 
 
    def stage_a_transform(self, bucket, key, team, dataset): 
        """Applies StageA Transformation to Object 
         
        Arguments: 
            bucket {string} -- Origin S3 Bucket 
            key {string} -- Key to transform
            team {string} -- Team owning the transformation 
            dataset {string} -- Dataset targeted by transformation 
        Returns: 
            {dict} -- Dictionary of Bucket and Keys transformed 
        """ 
        transform_info = self.get_transform_info('{}-{}'.format(team, dataset))
        module = import_module('datalake_library.transforms.stage_a_transforms.{}'.format(transform_info['stage_a_transform'])) 
        Transform = getattr(module, 'CustomTransform') 
        try: 
            response = Transform().transform_object(bucket, key, team, dataset) 
        except Exception as e:
            raise e 
         
        if ((not isinstance(response, list) or (len(response) == 0))): 
            raise ValueError("Invalid list of processed keys - Aborting") 
        else:
            logger.info("Object successfully transformed") 
 
        return response 
   
    def stage_b_transform(self, bucket, keys, team, dataset): 
        """Applies StageB Transformation to Objects 
         
        Arguments: 
            bucket {string} -- Origin S3 Bucket 
            keys {string} -- Keys to transform
            team {string} -- Team owning the transformation  
            dataset {string} -- Dataset targeted by transformation
        Returns: 
            {dict} -- Dictionary of Bucket, Keys transformed, Path to Keys Processed and Job Details 
        """ 
        transform_info = self.get_transform_info('{}-{}'.format(team, dataset)) 
        module = import_module('datalake_library.transforms.stage_b_transforms.{}'.format(transform_info['stage_b_transform'])) 
        Transform = getattr(module, 'CustomTransform') 
        try: 
            response = Transform().transform_object(bucket, keys, team, dataset) 
        except Exception as e:
            raise e 
         
        if ((len(response) == 0) or (not isinstance(response, dict)) or ('processedKeysPath' not in response)
         or ('jobDetails' not in response) or ('jobStatus' not in response['jobDetails'])):
            raise ValueError("Invalid dictionary - Aborting") 
         
        return response 
 
    def stage_b_job_status(self, bucket, keys, team, dataset, processed_keys_path, job_details): 
        """Checks completion of Stage B Job 
         
        Arguments: 
            bucket {string} -- Origin S3 bucket 
            keys {string} -- Keys to transform 
            processed_keys_path {string} -- Job output S3 path 
            job_details {string} -- Details about job to monitor
            team {string} -- Team owning the transformation   
            dataset {string} -- Dataset targeted by transformation
        Returns: 
            {dict} -- Dictionary of Bucket and Keys transformed 
        """ 
        transform_info = self.get_transform_info('{}-{}'.format(team, dataset)) 
        module = import_module('datalake_library.transforms.stage_b_transforms.{}'.format(transform_info['stage_b_transform']))
        Transform = getattr(module, 'CustomTransform') 
        try: 
            response = Transform().check_job_status(bucket, keys, processed_keys_path, job_details) 
        except Exception as e: 
            raise e 
         
        if ((len(response) == 0) or (not isinstance(response, dict)) or ('processedKeysPath' not in response)
         or ('jobDetails' not in response) or ('jobStatus' not in response['jobDetails'])): 
            raise ValueError("Invalid dictionary - Aborting") 
        if response['jobDetails']['jobStatus'] == 'FAILED':
            raise ValueError('Job Failed') 
        elif response['jobDetails']['jobStatus'] == 'SUCCEEDED': 
            logger.info("Objects successfully transformed") 
 
        return response 
 
    def get_transform_info(self, dataset):
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)      
        return dynamo_interface.get_transform_table_item(dataset)['transforms']
 
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
    StageToRedshiftOperator:
    
    Loads the data from the S3 to Redshift table.
    
    Inputs:
        
        redshift_conn_id: str       -> reshift connection id saved in the airflow connections
        aws_credentials_id: str     -> aws credentials id saved in the airflow connections
        table:str                   -> table name in redshift
        s3_bucket:str               -> s3 bucket name
        s3_key:str                  -> s3 key name
        file_format:str             -> Defining the File Format
        optional_parameters:list(str)     -> Additionla parameters for the copy statement
        
"""


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    FF_SET = ["CSV", "AVRO", "JSON", \
              "FIXEDWIDTH", "SHAPEFILE", \
              "PARQUET", "ORC", "DELIMITER"]
    
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 optional_parameters=[],
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
        self.optional_parameters = optional_parameters


    def execute(self, context):
        self.log.info('Starting to implement StageToRedshiftOperator')
        
        if self.file_format not in StageToRedshiftOperator.FF_SET:
            raise ValueError(f"InValid file format {self.file_format}. It should be any of {StageToRedshiftOperator.FF_SET}")
            
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            "\n\t".join(self.optional_parameters)
        )
        self.log.info(f"Copy SQL: {formatted_sql}")
        redshift.run(formatted_sql)
        self.log.info(f'Successful execution of StageToRedshiftOperator for {self.table}')

        
            
                
        
        






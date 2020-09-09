from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
    DataQualityOperator:
    
    Loads the dimension data from the staging Redshift table.
    
    Inputs:
        
        conn_id: str                                  -> reshift connection id saved in the airflow connections
        stmt_res:list(tuple(str, str))                -> SQL statement to be run to check the data and expected result
        
"""


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 stmt_res = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.stmt_res = stmt_res

    def execute(self, context):
        self.log.info('Implementing DataQualityOperator')
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        while(self.stmt_res):
            stmt, exp_res = self.stmt_res.pop()
            records = redshift.get_records(stmt)
            self.log.info(f'The records: {records}')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. Returned no results")
            act_res = records[0][0]
            if act_res!=exp_res:
                raise ValueError(f"Data quality check failed due to mismatch between actual and expected response")
            
            
            
            
            
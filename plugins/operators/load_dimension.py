from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
    LoadDimensionOperator:
    
    Loads the dimension data from the staging Redshift table.
    
    Inputs:
        
        conn_id: str                -> reshift connection id saved in the airflow connections
        sql_stmt:str                -> SQL statement to be run
        table:str                   -> target table name in redshift to insert dimension data
        
"""


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 sql_stmt = None,
                 table = None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_stmt = sql_stmt
        self.table = table
        

    def execute(self, context):
        self.log.info(f'Implementing the LoadDimensionOperator for {self.table}')
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info(f'Truncating {self.table}')
        redshift.run('TRUNCATE {}'.format(self.table))
            
        stmt = 'INSERT INTO {}'+' ( '+self.sql_stmt+' ) '
        redshift.run(stmt.format(self.table))
        self.log.info(f'Successful execution of LoadDimensionOperator for {self.table}')

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'


    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 sql_stmt = None,
                 table = None,
                 append_to_table = True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_stmt = sql_stmt
        self.table = table
        self.append_to_table = append_to_table
        

    def execute(self, context):
        self.log.info(f'Implementing the LoadFactOperator for {self.table}')
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        if not self.append_to_table:
            self.log.info(f'Truncating {self.table}')
            redshift.run('TRUNCATE {}'.format(self.table))
            
        stmt = 'INSERT INTO {}'+' ( '+self.sql_stmt+' ) '
        redshift.run(stmt.format(self.table))
        self.log.info(f'Successful execution of LoadFactOperator for {self.table}')

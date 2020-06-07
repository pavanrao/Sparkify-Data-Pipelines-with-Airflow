from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads fact table from staging tables
    Fact load is "Type 1", insert records.
    :param redshift_conn_id: Redshift connection ID
    :param table: fact table 
    :param select_sql: SQL query to fetch source data
    """
    
    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):

        self.log.info("Get redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Insert records into fact table in Redshift")
        self.log.info("Insert SQL:\n{table_insert_sql}")
        table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """
        redshift_hook.run(table_insert_sql)

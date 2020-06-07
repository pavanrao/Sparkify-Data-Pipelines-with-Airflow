from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Run data quality checks by running test SQL.
    This Operator is usued at the end of the batch to check data after load.
    
    :param redshift_conn_id: Redshift connection ID
    :param test_query: SQL query to run on Redshift data warehouse
    :param expected_result: Expected outcome of the test query

    """
    
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_query="",
                 expected_result="",
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result
    
    def execute(self, context):
        
        self.log.info("Get redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Run data quality test")
        records = redshift_hook.get_records(self.test_query)
        if records[0][0] != self.expected_result:
            raise ValueError(f"""
                Data quality check failed. \
                {records[0][0]} does not equal {self.expected_result}
            """)
        else:
            self.log.info("Data quality check passed")

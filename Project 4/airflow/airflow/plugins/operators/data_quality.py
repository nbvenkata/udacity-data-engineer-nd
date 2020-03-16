from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failing_tests = []
        
        self.log.info('starting to Data Quality checks %s')
        
        for check in self.dq_checks:
            sql = check.get('check sql')
            exp_result = check.get('expected_result')
            
            records = redshift.get_records(sql)[0]
            
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
                
        if error_count > 0:
            self.log.info('Test failed')
            self.log.info('Failing tests')
            raise ValueError('Data quality check failed')
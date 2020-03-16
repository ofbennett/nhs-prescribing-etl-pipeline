from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id="my_postgres_conn",
                 quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.quality_checks=quality_checks
        

    def execute(self, context):
        self.log.info('Starting data quality checks')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        failed_tests = []
        failed_num = 0
        for i, test in enumerate(self.quality_checks):
            result = redshift.get_first(test['sql'])[0]
            self.log.info("Running test {}".format(i))
            self.log.info("SQL: {}".format(test['sql']))
            self.log.info("Should be {}".format(test['shouldBe']))
            self.log.info("Value: {}".format(test['value']))
            self.log.info("Result: {}".format(result))
            if test['shouldBe'] == '==':
                if result == test['value']:
                    continue
                else:
                    failed_num +=1
                    failed_tests.append(i)
            elif test['shouldBe'] == '>':
                if result > test['value']:
                    continue
                else:
                    failed_num +=1
                    failed_tests.append(i)
            elif test['shouldBe'] == '<':
                if result < test['value']:
                    continue
                else:
                    failed_num +=1
                    failed_tests.append(i)
        if failed_num > 0:
            self.log.info("Date quality checks failed. The following tests failed:")
            self.log.info(failed_tests)
            raise ValueError("Data quality checks failed.")
        else:
            self.log.info("All data quality checks passed!")
            
                    
                    
        
        
        
        
        
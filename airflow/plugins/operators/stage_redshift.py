from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    sql_unformated = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'eu-west-2'
        CSV
        IGNOREHEADER 1;
        """

    template_fields = ['s3_key']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="my_redshift_conn",
                 aws_credentials_id="my_aws_conn",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 header=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.header = header

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        sql_formated = StageToRedshiftOperator.sql_unformated.format(self.table,
                                                                     s3_path,
                                                                     credentials.access_key,
                                                                     credentials.secret_key)
        if not self.header:
            sql_formated = sql_formated.replace('IGNOREHEADER 1','')
        redshift.run(sql_formated)
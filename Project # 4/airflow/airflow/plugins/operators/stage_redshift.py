from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
                                                                                                                            
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_name='',
                 s3_bucket='',
                 s3_key='',
                 path='',
                 delimiter='',
                 headers=1,
                 quote_char='',
                 file_type='',
                 aws_credentials={},
                 region='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.path = path
        self.delimiter = delimiter
        self.headers = headers
        self.quote_char = quote_char
        self.file_type = file_type
        self.aws_credentials = aws_credentials
        self.region = region        
   
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        print(s3_path)
        copy_statement = """
                    copy {} 
                    from '{}'
                    access_key_id '{}'
                    secret_access_key '{}'
                    region as '{}'
                 """.format(self.table_name, s3_path,
                            self.aws_credentials.get('key'), 
                            self.aws_credentials.get('secret'), self.region)

        if self.file_type == 'csv': 
            file_statement = """
            delimiter '{}'
            ignoreheader {}
            csv quote as '{}';
            """.format(self.delimiter, self.headers, self.quote_char)
                    
        if self.file_type == 'json':
            file_statement = "json 'auto';"
            if self.table_name == 'staging_events':
                file_statement = f"json '{self.path}'"
            full_copy_statement = '{} {}'.format(copy_statement,file_statement)
            self.log.info('Starting to copy data from S3')
            redshift.run(full_copy_statement)
            self.log.info('Staging done!')





from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from spark.jobs.extract_transform_load import extract_transform_load

class ETLOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        *args, **kwargs):
        super(ETLOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        extract_transform_load()
import unittest
from unittest.mock import patch
from datetime import datetime
from tempfile import TemporaryDirectory
import boto3
from moto import mock_s3

from airflow import DAG
from jo_airflow.plugins.operators.rmarkdown_to_s3_operator import RMarkdownToS3Operator

AWS_CONN_ID = 'aws_default'
S3_BUCKET = 'test-bucket'
S3_KEY = 'path/test_rmd_output.html'
RMD_INPUT = 'test_rmd_html_file.Rmd'
RMD_OUTPUT_FILE = 'test_rmd_output.html'
RMD_PARMAS = {'a_key': 'a_value', 'b_key': 'b_value'}

# Temporary directory to be used by mock_temporary_directory
mock_tmp_dir = TemporaryDirectory(prefix='airflowtest')
TMP_DIR = mock_tmp_dir.name
RMD_OUTPUT_PATH = TMP_DIR + '/' + RMD_OUTPUT_FILE
# Empty output file needed to satisfy S3 Hook
mock_rmd_output = open(RMD_OUTPUT_PATH, 'w')

class TestRMarkdownToS3Operator(unittest.TestCase):
    @patch("jo_airflow.plugins.operators.rmarkdown_to_s3_operator.TemporaryDirectory",
           autospec=True)
    @patch("jo_airflow.plugins.operators.rmarkdown_to_s3_operator.ListVector",
           autospec=True)
    @patch("jo_airflow.plugins.operators.rmarkdown_to_s3_operator.NULL",
           autospec=True)
    @patch('jo_airflow.plugins.operators.rmarkdown_to_s3_operator.importr',
           autospec=True)
    @mock_s3
    def test_execute(
        self,
        mock_importr,
        mock_null,
        mock_list_vector,
        mock_temporary_directory
    ):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=S3_BUCKET)

        mock_importr('rmarkdown').render.return_value = [RMD_OUTPUT_PATH]
        mock_temporary_directory.return_value.__enter__.return_value = TMP_DIR

        dag = DAG(dag_id='testdag', start_date=datetime.now())
        run_task = RMarkdownToS3Operator(
            rmd_input=RMD_INPUT,
            aws_conn_id=AWS_CONN_ID,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY,
            s3_replace=True,
            rmd_output_file=RMD_OUTPUT_FILE,
            rmd_params=RMD_PARMAS,
            dag=dag,
            task_id='test-test')
        run_task.execute(None)

        # Check importr calls rmarkdown library
        mock_importr.assert_called_with('rmarkdown')

        # Check params converted to R list vector
        mock_list_vector.assert_called_once_with(RMD_PARMAS)

        # Check render function called with correct args
        mock_importr('rmarkdown').render.assert_called_once_with(
            RMD_INPUT,
            output_format=mock_null,
            output_file=RMD_OUTPUT_FILE,
            output_dir=TMP_DIR,
            intermediates_dir=TMP_DIR,
            knit_root_dir=TMP_DIR,
            clean=True,
            params=mock_list_vector(RMD_PARMAS),
            encoding='UTF-8'
        )

        # Check if object was created in s3
        objects_in_dest_bucket = conn.list_objects(Bucket=S3_BUCKET)
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], S3_KEY)


if __name__ == '__main__':
    unittest.main()

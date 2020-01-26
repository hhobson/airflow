from rpy2.rinterface_lib.embedded import RRuntimeError
from rpy2.robjects.packages import importr
from rpy2.robjects import ListVector, NULL

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory


class RMarkdownToS3Operator(BaseOperator):
    """
    Render R Markdown document to the output format type and upload to S3.

    R Markdown documents can generate a range of output file types including
    HTML, PDF, Word and more. See R Markdown render documentation for more
    information -
    https://www.rdocumentation.org/packages/rmarkdown/versions/1.17/topics/render

        :param rmd_input: The input file to be rendered - can be an R script
            (.R), an R Markdown document (.Rmd), or a plain markdown document.
        :type rmd_input: str
        :param s3_bucket: S3 bucket to upload output file to. (templated)
        :type s3_bucket: str
        :param s3_key: S3 key that will point to the file. (templated)
        :type s3_key: str
        :param rmd_output_format: The R Markdown output format to convert to.
            If None will use first format type defined in YAML front-matter.
        :type rmd_output_format: str
        :param rmd_output_file: The name of the output file. If None then name
            will be based on input filename. (templated)
        :type rmd_output_file: str
        :param rmd_clean: Flag to decide whether to remove intermediate files
            created during rendering. If True will clean intermediate files.
        :type rmd_clean: bool
        :param rmd_params: Parameters that override custom params specified
            within the YAML front-matter. (templated)
        :type rmd_params: Dict
        :param rmd_encoding : The encoding of the input file.
        :type rmd_encoding: str
        :param aws_conn_id: source s3 connection.
        :type aws_conn_id: str
        :param s3_replace: Flag to decide whether to overwrite the key if it
            already exists. If False and the key exists, error will be raised.
        :type s3_replace: bool
        :param s3_verify: Whether verify SSL certificates for S3 connection.
            By default SSL certificates are verified.
            You can provide the following values:
            - ``False``: do not validate SSL certificates. SSL will still be
                used (unless use_ssl is False), but SSL certificates will not
                be verified.
            - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to
                uses. You can specify this argument if you want to use a
                different CA cert bundle than the one used by botocore.
        :type s3_verify: bool or str
    """

    template_fields = ('s3_bucket', 's3_key', 'rmd_output_file', 'rmd_params')

    @apply_defaults
    def __init__(
            self,
            rmd_input,
            aws_conn_id,
            s3_bucket,
            s3_key,
            rmd_output_format=None,
            rmd_output_file=None,
            rmd_clean=True,
            rmd_params=None,
            rmd_encoding='UTF-8',
            s3_replace=False,
            s3_verify=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rmd_input = rmd_input
        self.rmd_output_format = rmd_output_format or NULL
        self.rmd_output_file = rmd_output_file or NULL
        self.rmd_clean = rmd_clean
        self.rmd_params = rmd_params or NULL
        self.rmd_encoding = rmd_encoding
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_replace = s3_replace
        self.s3_verify = s3_verify

    def execute(self, context):
        """
        Render R Markdown document in temporary directory and upload it to S3
        """
        rmd = importr('rmarkdown')
        s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.s3_verify)

        # Convert Python dictionary to R list vector
        if self.rmd_params != NULL:
            self.rmd_params = ListVector(self.rmd_params)

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            try:
                render_rmd = rmd.render(
                    self.rmd_input,
                    # output_format=self._none_to_null(self.rmd_output_format),
                    output_format=self.rmd_output_format,
                    output_file=self.rmd_output_file,
                    output_dir=tmp_dir,
                    intermediates_dir=tmp_dir,
                    knit_root_dir=tmp_dir,
                    clean=self.rmd_clean,
                    params=self.rmd_params,
                    encoding=self.rmd_encoding
                )
                # TODO: Render can output multiple files, each of a different
                # file type. Need to add ability to deal with this
                render_output = render_rmd[0]
            except RRuntimeError as err:
                self.log.error('Received R error: {}'.format(err))
                # TODO: Need to exit operator
            self.log.info(
                'R Markdown output location: {}'.format(render_output))

            s3.load_file(
                render_output,
                self.s3_key,
                bucket_name=self.s3_bucket,
                replace=self.s3_replace
            )
            self.log.info('Uploaded to S3')

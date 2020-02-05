s3-log-aggregator
=================

Batch job that aggregates S3 access logs into a dataset. Proof-of-concept Spark code is in `spark-snippets.scala`, just copy into a running `spark-shell`.

Instructions for how to [set up Spark on local machine](https://github.oslo.kommune.no/pages/origo-dataplatform/spark-lokal-maskin.html).

The format of the S3 access logs are documented at https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html

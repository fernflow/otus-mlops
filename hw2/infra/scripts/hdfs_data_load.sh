# s3cmd sync --acl-public s3://otus-mlops-source-data/ s3://hw2-data-bucket/
hdfs dfs -mkdir -p /user/ubuntu/data
hadoop distcp s3a://hw2-data-bucket/ /user/ubuntu/data
hdfs dfs -ls /user/ubuntu/data
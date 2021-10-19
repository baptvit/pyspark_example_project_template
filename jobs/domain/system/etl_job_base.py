"""
etl_job_base.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/domain/system/etl_job_base.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""
import sys
import argparse
from dependencies.spark import start_spark
from jobs.domain.system.etl_job_base_functions import extract_data, transform_data, load_data


def main(args: list) -> None:
    """Main ETL script definition.

    Parameters:
        args (list): argumentos came from sys.args

    Returns:
        None:Returning value
    """
    # Parsing submmited variables
    job_name = set_up_args(args)

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name=job_name,
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    job(spark, log, config)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def job(spark: str, log: str, config: str) -> None:
    """Job ETL script definition.

    Parameters:
        spark (SparkSession): Main spark session for the job.
        log (Log4j): Logging instance.
        config (dict): config paramenters for the job

    Returns:
        None:Returning value
    """
    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, config['steps_per_floor'])
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    return None


def set_up_args(args: list) -> list:
    """Set up variables for the job.

    Parameters:
        args (lis): Main spark session for the job.

    Returns:
        list:List of variables 
    """
    parser = argparse.ArgumentParser(
        description='PySpark dummy template job args')
    parser.add_argument('-jbn', '--job_name_arg',
                        dest='job_name_arg',
                        type=str)

    args = parser.parse_args(args)

    return args.job_name_arg


# entry point for PySpark ETL application
if __name__ == '__main__':
    main(sys.args[1:])

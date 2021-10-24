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
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit


def main(args: list) -> None:
    """Main ETL script definition.

    Parameters:
        args (list): argumentos came from sys.args

    Returns:
        None:Returning value
    """
    # Parsing submmited variables
    job_name, steps_per_floor = set_up_args(args)

    # start Spark application and get Spark session, logger and config
    spark, log, config_dict= start_spark(
        app_name=job_name)

    # log that main ETL job is starting
    log.info('etl_job is up-and-running')

    # execute ETL pipeline
    job(spark, log, steps_per_floor)

    # log the success and terminate Spark application
    log.info('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.

    Parameters:
        spark (SparkSession): Main spark session for the job.

    Returns:
        SparkDataframe:Spark DataFrame from the parquet
    """
    return spark.read.parquet('file:///code/tests/domain/system/test_unit/test_data/employees')


def transform_data(df, steps_per_floor_):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    df_transformed = (
        df
        .select(
            col('id'),
            concat_ws(
                ' ',
                col('first_name'),
                col('second_name')).alias('name'),
               (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))

    return df_transformed


def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv('file:///code/tests/domain/system/test_integration/output_employees', mode='overwrite', header=True))
    return None


def create_test_data(spark, config):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
        Row(id=1, first_name='Dan', second_name='Germain', floor=1),
        Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', second_name='Lai', floor=2),
        Row(id=5, first_name='Stu', second_name='White', floor=3),
        Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', second_name='Bird', floor=4),
        Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet('file:///code/tests/domain/system/test_unit/test_data/employees', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet('file:///code/tests/domain/system/test_unit/test_data/employees_report', mode='overwrite'))

    return None


def job(spark: str, log: str, steps_per_floor: int) -> None:
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
    data.show()
    data_transformed = transform_data(data, steps_per_floor)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    return None


def set_up_args(args: list) -> list:
    """Set up variables for the job.

    Parameters:
        args (list): Main spark session for the job.

    Returns:
        list:List of variables 
    """
    parser = argparse.ArgumentParser(
        description='PySpark dummy template job args')
    parser.add_argument('-jbn', '--job_name_arg',
                        dest='job_name_arg',
                        type=str)
    parser.add_argument('-spf', '--steps_per_floor',
                        dest='steps_per_floor',
                        type=str)

    args = parser.parse_args(args)

    return args.job_name_arg, args.steps_per_floor


# entry point for PySpark ETL application
if __name__ == '__main__':
    main(sys.argv[1:])

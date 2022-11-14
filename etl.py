
from pyspark.sql.types import DateType
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import findspark
findspark.init('C:\spark')


def proccess_customer_data(df):
    """_summary_
        - Select Customer Data We Need
        - Load Customers Data To customers Table
    Args:
        df (Object): Spark Data
    """
    # Select Customer Data We Need
    print("Select Customer Data We Need....")
    customer_data = (df.select('customer_name', 'city', 'state', 'sentiment')
                     .withColumn('customer_id', monotonically_increasing_id()))
    customer_data = customer_data.select(
        'customer_id', 'customer_name', 'city', 'state', 'sentiment')
    print("Load Customers Data To customers Table....")

    # Load Customers Data To customers Table
    (customer_data.write.format("jdbc")
        .option("url", "jdbc:mysql://127.0.0.1:3306/call_center")
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "customers")
        .option("user", "root").option("password", "abdyassein").mode("append").save())


def proccess_time_data(df):
    """_summary_
        - Select call_timestamp And Convert it To TimeStamp Type
        - Select Data We Need
        - reorganization of the structure
        - Load call time Data To call_time Table
    Args:
        df (Object): Spark Data
    """
    # Select call_timestamp And Convert it To TimeStamp Type
    print(" Select call_timestamp And Convert it To TimeStamp Type....")
    time_data = (df.select('call_timestamp',
                           F.from_unixtime(F.unix_timestamp(
                                           'call_timestamp', 'MM/dd/yyy'))
                           .alias('date'))
                 .dropDuplicates())
    # Select Data We Need
    print("Select Data We Need....")
    time_data = (time_data.withColumn("day", F.dayofmonth("date"))
                 .withColumn('week', F.weekofyear('date'))
                 .withColumn('weekday', F.date_format("date", "E"))
                 .withColumn('month', F.month('date'))
                 .withColumn('year', F.year("date"))
                 .withColumn('call_date', F.to_date("date")))
    # reorganization of the structure
    print("reorganization of the structure....")
    time_data = time_data.select(
        'call_date', 'day', 'week', 'weekday', 'month', 'year')

    # Load call time Data To call_time Table
    print("Load call time Data To call_time Table....")
    (time_data.write.format("jdbc")
        .option("url", "jdbc:mysql://127.0.0.1:3306/call_center")
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "call_time")
        .option("user", "root").option("password", "abdyassein").mode("append").save())


def proccess_call_info_data(df):
    """_summary_
        - Select Data We Need
        - Load call info Data To call_info Table
    Args:
         df (Object): Spark Data
    """
    # Select Data We Need
    print("Select Data We Need....")
    call_info = df.selectExpr(
        'id as call_id', 'reason', 'channel', 'call_center', 'response_time')
    # Load call info Data To call_info Table
    print("Load call info Data To call_info Table....")
    (call_info.write.format("jdbc")
        .option("url", "jdbc:mysql://127.0.0.1:3306/call_center")
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "call_info")
        .option("user", "root").option("password", "abdyassein").mode("append").save())


def proccess_call_fact_data(df):
    """_summary_
        - Select Data We Need With Specific Data Type
        - Replaice Null Valu With Zero
        - reorganization of the structure
        - Load call fact Data To call_fact Table
    Args:
         df (Object): Spark Data
    """
    # Select Data We Need With Specific Data Type
    print("Select Data We Need With Specific Data Type....")
    call_data = (df
                 .select(F.col('id').alias('call_id'), F.to_date(F.from_unixtime(F.unix_timestamp('call_timestamp', 'MM/dd/yyy')))
                         .alias('call_date'), F.col('csat_score').cast('int'), F.col('call duration in minutes').cast('int').alias("call_in_minutes"))
                 .withColumn('customer_id', monotonically_increasing_id()))
    # Replaice Null Valu With Zero
    print("Replaice Null Valu With Zero....")
    call_data = call_data.fillna({'csat_score': '0'})
    # reorganization of the structure
    print("reorganization of the structure....")
    call_data = call_data.select(
        'call_id', 'Customer_id', 'call_date', 'csat_score', 'call_in_minutes')
    # Load call fact Data To call_fact Table
    print("Load call fact Data To call_fact Table....")
    (call_data.write.format("jdbc")
        .option("url", "jdbc:mysql://127.0.0.1:3306/call_center")
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "call_fact")
        .option("user", "root").option("password", "abdyassein").mode("append").save())


def main():
    run_create_tables = input(
    '\nWould you like to run create_tables.py ? Enter yes or no.\n')
    if run_create_tables.lower() != 'yes':
        print("Run create_tables.py...")
        exec(open('create_tables.py').read())
    # Create New Spark Session With app Name call center
    print("Create New SparkSession Object.... ")
    spark = (SparkSession
             .builder
             .appName("call-center")
             .config("spark.jars", "mysql-connector-java-5.1.47.jar")
             .master("local").getOrCreate())

    # Read Call Center DataSer From Path
    print("Read DataSet From Path...")
    call_center_df = spark.read.csv("data/CallCenter.csv", header=True)

    print("Proccess Customers Data...")
    proccess_customer_data(call_center_df)
    print("Proccess Time Data...")
    proccess_time_data(call_center_df)
    print("Proccess Call Info Data...")
    proccess_call_info_data(call_center_df)
    print("Proccess Call Fact Data...")
    proccess_call_fact_data(call_center_df)
    print("Spark Stop....")
    spark.stop()
    print("Successfully ETL Data ):")


if __name__ == "__main__":
    main()

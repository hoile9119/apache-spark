# Spark Structured Streaming for Spark 3

## **Introduction**

Welcome to Spark 3 Structured Streaming guide! This project aims to guide/walk you through how to set up packages and environment for your spark 3 streaming application

## **Features**

- **Real-time Data Processing**: Process data streams in real-time, providing low-latency and up-to-date insights.
- **Scalability**: Leverage the power of Apache Spark to handle large-scale data processing efficiently.
- **Easy-to-Use API**: The project comes with a simple and intuitive API, making it easy for developers to write streaming applications.
- **Integration**: Easily integrate with various data sources and sinks, such as Kafka, HDFS, S3, and more.
- **Fault Tolerance**: Spark's built-in fault-tolerance ensures that your streaming jobs can recover from failures gracefully.
- **Windowing and Aggregation**: Perform window-based computations and aggregations on data streams effortlessly.
- **Python Package Management:** When you want to run your PySpark application on a cluster such as YARN, Kubernetes, Mesos, etc., you need to make sure that your code and all used libraries are available on the executors.

## **Installation ( for Spark 3.3.0 )**

To use the Spark3-Structured Streaming in your project, you can include these packages below as dependencies using Maven or SBT. Here's how:

- **Maven**
    
    ```xml
    <dependencies>
        <!-- Spark SQL Kafka -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.12</artifactId>
            <version>3.3.0</version>
        </dependency>
    
        <!-- Kafka Clients -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>3.3.0</version>
        </dependency>
    
        <!-- Spark Streaming Kafka -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming-kafka-0-10_2.12</artifactId>
            <version>3.3.0</version>
        </dependency>
    
        <!-- Spark Token Provider Kafka -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-token-provider-kafka-0-10_2.12</artifactId>
            <version>3.3.0</version>
        </dependency>
    
        <!-- Commons Pool 2 -->
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-pool2</artifactId>
            <version>2.11.1</version>
        </dependency>
    </dependencies>
    ```
    
- **SBT**
    
    In your **`build.sbt`** file, add the following library dependencies:
    
    ```scala
    libraryDependencies ++= Seq(
      // Spark SQL Kafka
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0",
    
      // Kafka Clients
      "org.apache.kafka" % "kafka-clients" % "3.3.0",
    
      // Spark Streaming Kafka
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.0",
    
      // Spark Token Provider Kafka
      "org.apache.spark" %% "spark-token-provider-kafka-0-10" % "3.3.0",
    
      // Commons Pool 2
      "org.apache.commons" % "commons-pool2" % "2.11.1"
    )
    ```
    

## **Usage**

Using Spark-Structured Streaming is straightforward. Start by importing the necessary classes and create a **`SparkSession`**:

## **Examples**

Here are some examples to get you started:

1. **Python Package Management**
    1. References:
        1. [official spark source](https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html)
        2. [venv-pack source](https://jcristharif.com/venv-pack/spark.html)
    2. Examples
        
        Create an environment & Activate the environment & Install some packages into the environment
        
        ```bash
        # create
        python -m venv example
        
        # activate
        source example/bin/activate
        
        # install packages
        pip install numpy pandas scikit-learn scipy
        
        # install venv-pack
        pip install venv-pack
        ```
        
        Package the environment into a `tar.gz` archive:
        
        ```bash
        venv-pack -o environment.tar.gz
        ```
        
        Submit the job to Spark using `spark3-submit`. In YARN cluster mode:
        
        ```bash
        PYSPARK_PYTHON=./environment/bin/python \
        spark3-submit \
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
        --master yarn \
        --deploy-mode cluster \
        --archives environment.tar.gz#environment \
        script.py
        ```
        
        Write a PySpark script, for example:
        
        ```bash
        # script.py
        # import packages in tar.gz archive after extracting it, this case is using python 3.9 
        import site
        site.addsitedir("./environment/lib/python3.9/site-packages")
        
        from pyspark import SparkConf
        from pyspark import SparkContext
        
        conf = SparkConf()
        conf.setAppName('spark-yarn')
        sc = SparkContext(conf=conf)
        
        def some_function(x):
            # Packages are imported and available from your bundled environment.
            import sklearn
            import pandas
            import numpy as np
        
            # Use the libraries to do work
            return np.sin(x)**2 + 2
        
        rdd = (sc.parallelize(range(1000))
                 .map(some_function)
                 .take(10))
        
        print(rdd)
        ```
        

1. **Sink sources: foreach,  foreachBatch**
    1. foreach (  process by row )
        
        ```python
        def process_row(row):
        	pass
        
        # Create Kafka consumer DataFrame
        df = spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
        
        # Process the Kafka DataFrame
        
        output = df\
          .selectExpr("cast(key as string) as key","cast(value as string) as value")
        
        query = output.writeStream \
        		.outputMode("append") \
        		.foreach(process_row)\
        		.option("checkpointLocation", "checkpointLocation") \
        .start()
        
        query.awaitTermination()
        ```
        
    2. foreachBatch ( process by batch or df )
        
        ```python
        def process_batch(df, epoch_id):
        	pass
        
        # Create Kafka consumer DataFrame
        df = spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
        
        # Process the Kafka DataFrame
        
        output = df\
          .selectExpr("cast(key as string) as key","cast(value as string) as value")
        
        query = output.writeStream \
        		.trigger(processingTime="30 seconds") \
        		.outputMode("append") \
        		.foreachBatch(lambda df, epochId: process_batch(df, epochId))\
        		.start()
        
        query.awaitTermination()
        ```
        
2. ****Avro Schema Deserializer****
    1. Confluent Avro message
        
        ```python
        import avro
        import avro.io
        import avro.schema
        import io
        
        avro_schema = avro.schema.parse(open('avro_schema.txt').read())
        
        # other way is to read schema registry -> not show here -> will update later
        
        bytes_reader = io.BytesIO(row.value)
        bytes_reader.seek(5)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(avro_schema)
        record = reader.read(decoder)
        ```
        
    2. Avro message
   
4. **XML string parsing using spark-xml**
    
    ```python
    
    from pyspark.sql.column import Column, _to_java_column
    from pyspark.sql.types import _parse_datatype_json_string
    from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
    from pyspark.sql.functions import col, expr
    
    def ext_from_xml(xml_column, schema, options={}):
        java_column = _to_java_column(xml_column.cast('string'))
        java_schema = spark._jsparkSession.parseDataType(schema.json())
        scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
        jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
            java_column, java_schema, scala_map)
        return Column(jc)
    
    def ext_schema_of_xml_df(df, options={}):
        assert len(df.columns) == 1
    
        scala_options = spark._jvm.PythonUtils.toScalaMap(options)
        java_xml_module = getattr(getattr(
            spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$")
        java_schema = java_xml_module.schema_of_xml_df(df._jdf, scala_options)
        return _parse_datatype_json_string(java_schema.json())
    
    schema = ext_schema_of_xml_df(df,options={})
    df = df.withColumn("parsed",ext_from_xml(expr("value"),schema)).selectExpr("parsed.*")
    ```
    

## **Contributing**

We welcome contributions from the community! If you find any issues or have ideas to improve the project, please feel free to open an issue or submit a pull request. Make sure to read our **[contribution guidelines](https://chat.openai.com/CONTRIBUTING.md)** before getting started.

## **License**

This project is licensed under the **[MIT License](https://chat.openai.com/LICENSE)**. Feel free to use and distribute it according to the terms specified in the license.
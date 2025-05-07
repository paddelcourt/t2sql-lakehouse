FROM apache/airflow:2.9.2
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY quarto.sh /
RUN cd / && bash /quarto.sh

COPY setup_conn.py $AIRFLOW_HOME

User root

# RUN python $AIRFLOW_HOME/setup_conn.py
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#     default-jdk

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk

# export JAVA_HOME='/usr/lib/jvm/java-17-openjdk-amd64'
# export PATH=$PATH:$JAVA_HOME/bin
# export SPARK_HOME='/opt/spark'
# export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
RUN curl https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark-3.5.1-bin-hadoop3.tgz

# Change permissions of the downloaded tarball
RUN chmod 755 spark-3.5.1-bin-hadoop3.tgz

# Create the target directory and extract the tarball to it
RUN mkdir -p /opt/spark && tar xvzf spark-3.5.1-bin-hadoop3.tgz --directory /opt/spark --strip-components=1


# Download Iceberg Spark runtime JAR
RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.3_2.12-1.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar

# Add default Iceberg configs to Spark (optional but recommended)
RUN echo "\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\n\
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog\n\
spark.sql.catalog.spark_catalog.type=hive\n\
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog\n\
spark.sql.catalog.local.type=hadoop\n\
spark.sql.catalog.local.warehouse=/opt/spark/iceberg-warehouse\n\
spark.hadoop.hive.cli.print.header=true\n\
" >> /opt/spark/conf/spark-defaults.conf



ENV JAVA_HOME='/usr/lib/jvm/java-17-openjdk-arm64'
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME='/opt/spark'
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin


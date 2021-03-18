FROM jupyter/pyspark-notebook

RUN python3 -m pip install prefect

# Make Spark less verbose
USER root
RUN sed 's/log4j.rootCategory=INFO/log4j.rootCategory=ERROR/' \
    /usr/local/spark/conf/log4j.properties.template > /usr/local/spark/conf/log4j.properties

USER jovyan

COPY . .

CMD ["spark-submit", "pipeline.py"]

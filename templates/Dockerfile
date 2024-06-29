# {{templates.disclaimer}}
FROM apache/spark:3.4.1-scala2.12-java11-ubuntu

USER root

RUN mkdir -p /opt/{{general.name}} && \
    mkdir -p /opt/{{general.name}}/logs && \
    mkdir -p /opt/{{general.name}}/keyfile && \
    mkdir -p /opt/spark/conf && \
    chown -R spark:spark /opt/{{general.name}} && \
    chmod -R 777 /opt/{{sparkbench}}

# COPY foo bar/foo copies foo into bar
# COPY foo bar/ copies contents of foo into bar
# https://stackoverflow.com/questions/26504846/copy-directory-to-another-directory-using-add-command
COPY docker/jars /opt/{{general.name}}/jars
COPY docker/tpcds-bin /opt/{{general.name}}/tpcds-bin
COPY spark-defaults.conf /opt/spark/conf/
COPY target/scala-2.12/sparkbench_2.12-1.0.jar /opt/{{general.name}}/
COPY docker/entrypoint.sh /opt/{{general.name}}/entrypoint.sh
COPY docker/queries /opt/{{general.name}}/queries
COPY docker/workloads /opt/{{general.name}}/workloads
COPY docker/log4j2.properties /opt/{{general.name}}/jars/

# Only use outside google cloud if accessing google cloud storage
# ENTRYPOINT [ "/opt/{{general.name}}/entrypoint.sh" ]
ENTRYPOINT [ "/opt/entrypoint.sh" ]

USER 185

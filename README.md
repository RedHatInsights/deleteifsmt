# Kafka connect SMT to conditionally delete message
This java lib implements Kafka connect SMT (Single Message Transformation) to
delete message value (set to `null`) for some condition.

## Config
Use it in connector config file like this:
~~~json
...
"transforms": "deleteIf",
"transforms.deleteif.type": "com.redhat.insights.deleteifsmt.DeleteIf$Value",
...
~~~

## Install to Kafka Connect
After build copy file `target/kafka-connect-smt-deleteifsmt-0.0.1-jar-with-dependencies.jar`
to Kafka Connect container `` copying to its docker image or so.

It can be done adding this line to Dockerfile:
~~~Dockerfile
COPY ./kafka-connect-smt-deleteifsmt-0.0.1-jar-with-dependencies.jar $KAFKA_CONNECT_PLUGINS_DIR
~~~

Or download current release:
~~~Dockerfile
RUN curl -fSL -o /tmp/plugin.tar.gz \
    https://github.com/RedHatInsights/deleteifsmt/releases/download/0.0.1/kafka-connect-smt-deleteifsmt-0.0.1.tar.gz && \
    tar -xzf /tmp/plugin.tar.gz -C $KAFKA_CONNECT_PLUGINS_DIR && \
    rm -f /tmp/plugin.tar.gz;
~~~

## Example
~~~bash
# build jar file and store to target directory
mvn package

# start example containers (kafka, postgres, elasticsearch, ...)
docker-compose up --build

# when containers started run in separate terminal:
cd dev
./connect.sh # init postgres and elasticsearch connectors
./show_topics.sh # check created topic 'dbserver1.public.hosts' in kafka
./show_es.sh # check transformed documents imported from postgres to elasticsearch
./db_inventory.sh # enter sql console and run "delete from hosts where id = 1;"
./show_es.sh # see that document with id 1 is deleted

# ... stop containers
docker-compose down
~~~

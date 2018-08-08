# Avro
This project uses Avro for managing schema's and data types common when working
with Apache Kafka. The application utilize the generated POJOs 
when pulling data from a topic or pushing data to a topic. 

### Generating POJOs

The `.avsc` files in this directory are converted to POJO's via
a gradle plugin during the build.

https://github.com/commercehub-oss/gradle-avro-plugin

### Schema Registry

The Avro schemas are tracked in the schema registry. If you are running
the docker stack locally, you can interact with the schema-registry's API
to get or update schemas.
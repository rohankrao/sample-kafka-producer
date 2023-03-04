## Install Schema Registry

helm upgrade --install sr -n kafka cp-schema-registry --set kafka.external=true --set kafka.address=my-cluster-kafka-brokers:9092

## Sample Vector Config

    [sources.in]
    type = "stdin"
    
    [sources.docker_source]
    type = "docker_logs"
    docker_host = "http://<IP>:2375"
    
    [sources.demo_logs]
    type = "demo_logs"
    format = "syslog"
    
    
    [sinks.out]
    type = "http"
    inputs = [ "demo_logs"]
    uri = "http://<IP>:3333/produce"
    compression = "none"
    
    [sinks.out.encoding]
    codec = "json"
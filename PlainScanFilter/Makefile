run_kafka:
	@ /usr/local/kafka/bin/zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties
	@ /usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties

create_topics:
	@ /usr/local/kafka/bin/kafka-topics.sh --create --topic point-cloud --bootstrap-server localhost:9092 --partitions 16

stop_kafka:
	@ /usr/local/kafka/bin/kafka-server-stop.sh || echo ""
	@ /usr/local/kafka/bin/zookeeper-server-stop.sh || echo ""

clear_kafka:
	@ rm -rf /tmp/kafka-logs /tmp/zookeeper

write_to_topic:
	@ /usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic point-cloud

read_from_topic:
	@ /usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic point-cloud --from-beginning

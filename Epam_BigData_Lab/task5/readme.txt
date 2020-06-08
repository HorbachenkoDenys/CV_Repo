java -jar com.horbachenkodenis.KafkaProducer-assembly-0.1.jar --localhost:9092,localhost:9091 --test --Speaker1,Speaker2 --input/dictionary.txt

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class com.horbachenkodenis.App --master local[*] --deploy-mode client myconsumer_2.11-0.1.jar --localhost:9092,localhost:9091 --test --input/censored.txt --2
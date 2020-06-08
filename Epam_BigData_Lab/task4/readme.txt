spark-submit --class com.horbachenkodenis.SparkCoreTopPlayersCounter --master yarn --deploy-mode client topRDDplayers_2.11-0.1.jar /user/epam/task4/inputData /user/epam/task4/

spark-submit --class com.horbachenkodenis.SparkSqlTopPlayersCounter --master yarn --deploy-mode client sql_2.11-0.1.jar /user/epam/task4/inputData /user/epam/task4/

spark-submit --class com.horbachenkodenis.SparkDataSetTopPlayersCounter --master yarn --deploy-mode client sparkdatasettopplayercounter_2.11-0.1.jar /user/epam/task4/inputData /user/epam/task4/

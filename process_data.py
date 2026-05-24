from pyspark.sql import SparkSession

# Создание Spark сессии
spark = SparkSession.builder.appName("YandexCloudDemo").getOrCreate()

# Создание простого DataFrame с данными
data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Простая трансформация: добавим столбец с удвоенным возрастом
df_transformed = df.withColumn("DoubleAge", df.Age * 2)

# Показываем результат в логах (для демонстрации)
df_transformed.show()

# Сохраняем результат в output-бакет
output_path = "s3a://output-bucket-123456/result/"
df_transformed.write.mode("overwrite").csv(output_path)

spark.stop()

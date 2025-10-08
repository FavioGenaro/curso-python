import os, sys
from pyspark import SparkConf, SparkContext

# Usa el mismo intérprete de Python de VSCode
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Configuración estable para Windows
conf = (SparkConf()
        .setAppName("TestSpark")
        .setMaster("local[1]")
        .set("spark.python.worker.faulthandler.enabled", "true"))

sc = SparkContext(conf=conf)

# Ejemplo simple
rdd_primos = sc.parallelize([2, 3, 5, 7, 11, 13, 17])
rdd_filtrado = rdd_primos.filter(lambda x: x > 10)
print(rdd_filtrado.collect())

sc.stop()
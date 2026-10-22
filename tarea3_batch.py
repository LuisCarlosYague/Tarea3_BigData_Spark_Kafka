from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

spark = SparkSession.builder.appName('Tarea3_Procesamiento_Batch').master('local[*]').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

print('\n===== CARGA DE DATOS =====')
df = spark.read.csv('ventas.csv', header=True, inferSchema=True)
df.printSchema()
df.show(10, truncate=False)

print('\n===== LIMPIEZA DE DATOS =====')
df = df.dropDuplicates(['id_venta']).dropna(subset=['producto', 'categoria', 'cantidad', 'precio_unitario'])
df = df.filter((F.col('cantidad') > 0) & (F.col('precio_unitario') > 0))
print('Registros validos:', df.count())

print('\n===== TRANSFORMACION =====')
df = df.withColumn('total_venta', F.col('cantidad') * F.col('precio_unitario'))
df.show(10, truncate=False)

print('\n===== ESTADISTICAS =====')
df.select('cantidad', 'precio_unitario', 'total_venta').summary().show()

print('\n===== VENTAS POR CATEGORIA =====')
ventas_categoria = df.groupBy('categoria').agg(F.sum('total_venta').alias('total'), F.count('*').alias('numero_ventas')).orderBy(F.desc('total'))
ventas_categoria.show(truncate=False)

print('\n===== PRODUCTOS MAS VENDIDOS =====')
productos = df.groupBy('producto').agg(F.sum('cantidad').alias('unidades_vendidas')).orderBy(F.desc('unidades_vendidas'))
productos.show(truncate=False)

print('\n===== GENERACION DE GRAFICAS =====')
os.makedirs('graficas', exist_ok=True)
datos_categoria = ventas_categoria.toPandas()
plt.figure(figsize=(10, 6))
plt.bar(datos_categoria['categoria'], datos_categoria['total'])
plt.title('Ventas totales por categoria')
plt.xlabel('Categoria')
plt.ylabel('Total de ventas')
plt.tight_layout()
plt.savefig('graficas/ventas_categoria.png')
plt.close()

datos_productos = productos.toPandas()
plt.figure(figsize=(11, 6))
plt.bar(datos_productos['producto'], datos_productos['unidades_vendidas'])
plt.title('Unidades vendidas por producto')
plt.xlabel('Producto')
plt.ylabel('Unidades')
plt.xticks(rotation=35)
plt.tight_layout()
plt.savefig('graficas/productos_vendidos.png')
plt.close()

ventas_categoria.coalesce(1).write.mode('overwrite').option('header', True).csv('resultados/ventas_categoria')
productos.coalesce(1).write.mode('overwrite').option('header', True).csv('resultados/productos')
print('\nPROCESAMIENTO BATCH FINALIZADO')
print('Graficas y resultados guardados.')
spark.stop()

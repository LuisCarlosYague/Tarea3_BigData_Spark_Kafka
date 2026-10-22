import csv
import random
from datetime import datetime, timedelta

random.seed(42)
productos = [
    ('Computador', 'Tecnologia', 2500000),
    ('Celular', 'Tecnologia', 1200000),
    ('Televisor', 'Tecnologia', 1800000),
    ('Camisa', 'Ropa', 80000),
    ('Pantalon', 'Ropa', 120000),
    ('Zapatos', 'Calzado', 180000),
    ('Tenis', 'Calzado', 250000),
]
inicio = datetime(2026, 1, 1)
with open('ventas.csv', 'w', newline='', encoding='utf-8') as archivo:
    escritor = csv.writer(archivo)
    escritor.writerow(['id_venta', 'fecha', 'producto', 'categoria', 'cantidad', 'precio_unitario'])
    for i in range(1, 1001):
        producto, categoria, precio = random.choice(productos)
        fecha = inicio + timedelta(days=random.randint(0, 180))
        escritor.writerow([i, fecha.strftime('%Y-%m-%d'), producto, categoria, random.randint(1, 5), precio])
print('Archivo ventas.csv generado correctamente')

import time
import json
from kafka import KafkaProducer
import random

latencies = []
devices = []

class SDP:
    def __init__(self, device_id, intervalo_tiempo):
        self.device_id = device_id
        self.intervalo_tiempo = intervalo_tiempo


    def enviar_idProducto(self):
        while True:
            timestamp = time.time()
            valor = random.randint(1, 1000)
            mensaje = {
                'timestamp': timestamp,
                'device_id': self.device_id,
                'idProducto': valor
            }
            return mensaje

    def enviar_stock(self):
        while True:
            timestamp = time.time()
            valor = random.randint(1, 100)
            mensaje = {
                'timestamp': timestamp,
                'device_id': self.device_id,
                'stock': valor
            }
            return mensaje
    
    def enviar_venta(self):
        while True:
            timestamp = time.time()
            ultima_venta = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            mensaje = {
                'timestamp': timestamp,
                'device_id': self.device_id,
                'venta': ultima_venta
            }
            return mensaje
    
    def enviar_ubicacion(self):
        while True:
            timestamp = time.time()
            valor = random.randint(1, 30)
            mensaje = {
                'timestamp': timestamp,
                'device_id': self.device_id,
                'ubicacion': valor
            }
            return mensaje

    def send_data(self):

        servidores_bootstrap = 'kafka:9092'
        
        #se generan los productores
        producer1 = KafkaProducer(bootstrap_servers=[servidores_bootstrap], value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks='all', retries=4)
        producer2 = KafkaProducer(bootstrap_servers=[servidores_bootstrap], value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks='all', retries=4)
        producer3 = KafkaProducer(bootstrap_servers=[servidores_bootstrap], value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks='all', retries=4)
        producer4 = KafkaProducer(bootstrap_servers=[servidores_bootstrap], value_serializer=lambda v: json.dumps(v).encode('utf-8'), acks='all', retries=4)
        
        start_time = time.time()
        
        #enviar los datos desde producer y limpiar el buffer
        producer1.send('idProducto', value=json.dumps(self.enviar_idProducto()))
        producer2.send('stock', value=json.dumps(self.enviar_stock()))
        producer3.send('venta', value=json.dumps(self.enviar_venta())) 
        producer4.send('ubicacion', value=json.dumps(self.enviar_ubicacion()))

        producer1.flush()
        producer2.flush()
        producer3.flush()
        producer4.flush()
   
        end_time = time.time()

        #arreglo de latencias
        latency = end_time - start_time
        latencies.append(latency)

        print('Latencia: ' + str(latency))



# Configuración del sistema
n = 1000 # Número de dispositivos IoT
intervalo_tiempo = 2  # Intervalo de tiempo 

# Creación y ejecución de los dispositivos
print('intento nuevo')
for i in range(n):
    device = SDP(device_id=f"{i+1}", intervalo_tiempo=intervalo_tiempo)
    devices.append(device)
    device.send_data()

#creación de archivo para gráfica
with open('datos.txt', 'w') as file:
    for mensaje in latencies:
        file.write(str(mensaje) + '\n')

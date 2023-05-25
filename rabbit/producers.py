import time
import json
import pika 
import random

latencies = []


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
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) 

        channel1 = connection.channel() 
        channel1.queue_declare(queue='idProducto') 
        
        channel2 = connection.channel()
        channel2.queue_declare(queue='stock')

        channel3 = connection.channel()
        channel3.queue_declare(queue='venta')

        channel4 = connection.channel()
        channel4.queue_declare(queue='ubicacion')
        
        start_time = time.time()
        
        
        channel1.basic_publish(exchange='',
                                routing_key='idProducto',
                                body=json.dumps(self.enviar_idProducto()))
        
        channel2.basic_publish(exchange='',
                                routing_key='stock',
                                body=json.dumps(self.enviar_stock()))
        
        channel3.basic_publish(exchange='',
                                routing_key='venta',
                                body=json.dumps(self.enviar_venta()))
        
        channel4.basic_publish(exchange='',
                                routing_key='ubicacion',
                                body=json.dumps(self.enviar_ubicacion()))
        
               
        print(" [x] Sent %r" % json.dumps(self.enviar_idProducto()))
        print(" [x] Sent %r" % json.dumps(self.enviar_stock()))
        print(" [x] Sent %r" % json.dumps(self.enviar_venta()))
        print(" [x] Sent %r" % json.dumps(self.enviar_ubicacion()))

        
 
        end_time = time.time()
        latency = end_time - start_time
        latencies.append(latency)      
        connection.close()

           
        
        

n = 3000 # Número de dispositivos IoT
intervalo_tiempo = 2  
devices = []
print('intento nuevo')
for i in range(n):
    device = SDP(device_id=f"{i+1}", intervalo_tiempo=intervalo_tiempo)
    devices.append(device)
    device.send_data()
    
with open('datos.txt', 'w') as file:
    for data in latencies:
        file.write(str(data) + '\n')

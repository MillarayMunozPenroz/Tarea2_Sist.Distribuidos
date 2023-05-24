import time
import json
import pika #libreria necesaria para hacer uso de rabbitmq en python
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
        json_data1 = json.dumps(self.enviar_idProducto())
        json_data2 = json.dumps(self.enviar_stock())
        json_data3 = json.dumps(self.enviar_venta())
        json_data4 = json.dumps(self.enviar_ubicacion())

        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) #define donde estara el broker, en este caso, en la maquina local

        channel1 = connection.channel() #crea un nuevo canal de comunicacion 
        channel1.queue_declare(queue='idProducto') #Declaracion del nombre de la cola
        
        channel2 = connection.channel()
        channel2.queue_declare(queue='stock')

        channel3 = connection.channel()
        channel3.queue_declare(queue='venta')

        channel4 = connection.channel()
        channel4.queue_declare(queue='ubicacion')
        
        start_time = time.time()
        
            #se da forma al mensaje que se quiere mandar mediante un intercambio de un string vacio, indicando
            #tambien el nombre de la cola => queue='...', ademas del contenido del mensaje
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

        
        #variable para ayudar a calcular la latencia de los mensajes    
        end_time = time.time()

        #arreglo que contendra todas las latencias para posteriormente exportarlas y graficarlas
        latency = end_time - start_time
        latencies.append(latency)

            #paso final para finalizar nuestra aplicacion
        connection.close()

            # Aquí se realiza la lógica para enviar el json_data a través de la comunicación IoT
            # Puedes adaptar esta parte según el sistema que utilices para enviar los datos

            #print("Datos enviados:", json_data)
        
        
# Configuración del sistema
n = 30 # Número de dispositivos IoT
intervalo_tiempo = 2  # Intervalo de tiempo en segundos entre cada envío de datos

# Creación y ejecución de los dispositivos
devices = []

print('intento nuevo')
for i in range(n):
    device = SDP(device_id=f"{i+1}", intervalo_tiempo=intervalo_tiempo)
    devices.append(device)
    device.send_data()
    
with open('datosRabbitMQ.txt', 'w') as file:
    for data in latencies:
        file.write(str(data) + '\n')
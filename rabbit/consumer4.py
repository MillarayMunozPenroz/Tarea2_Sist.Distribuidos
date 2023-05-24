import pika
import time

#conexion al servidor RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

#declaracion de la cola de mensajeria
channel.queue_declare(queue='ubicacion')

#funcion para procesar los mensajes recibidos
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())
    time.sleep(body.count(b'.'))
    print(" [x] Done")

#declaracion para el consumo de los mensajes de la cola    
channel.basic_consume(queue='ubicacion', on_message_callback=callback, auto_ack=True)

print("[*] Waiting for messages. To exit press CTRL+C")

#se da inicio al consumo de mensajes de la cola
channel.start_consuming()

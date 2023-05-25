import pika
import time


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()


channel.queue_declare(queue='stock')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())
    time.sleep(body.count(b'.'))
    print(" [x] Done")

   
channel.basic_consume(queue='stock', on_message_callback=callback, auto_ack=True)

print("[*] Waiting for messages. To exit press CTRL+C")


channel.start_consuming()

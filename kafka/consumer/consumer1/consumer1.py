from kafka import KafkaConsumer

servidores_bootstrap = 'kafka:9092'
topic = 'idProducto'

consumidor = KafkaConsumer(topic, bootstrap_servers=[servidores_bootstrap])

for mensaje in consumidor:
    print("recibido en 1")
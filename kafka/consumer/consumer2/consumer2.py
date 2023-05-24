from kafka import KafkaConsumer

servidores_bootstrap = 'kafka:9092'
topic = 'stock'

consumidor = KafkaConsumer(topic, bootstrap_servers=[servidores_bootstrap])

for mensaje in consumidor:
    print("recibido en 2")
from confluent_kafka import Consumer, KafkaException

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'  # Lê as mensagens do início
}

consumer = Consumer(conf)
topic = "hello_world"
consumer.subscribe([topic])

print("Aguardando mensagens...")

try:
    while True:
        msg = consumer.poll(1.0)  # Aguarda mensagens
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        print(f"Mensagem recebida: {msg.value().decode()}")

except KeyboardInterrupt:
    print("Encerrando consumidor...")
finally:
    consumer.close()

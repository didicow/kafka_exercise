from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}  # Conectando no Kafka

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'Erro ao enviar mensagem: {err}')
    else:
        print(f'Mensagem enviada: {msg.value().decode()} para {msg.topic()} [{msg.partition()}]')

topic = "hello_world"

for i in range(10):
    message = f"Mensagem {i}"
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.flush()  # Garante que a mensagem seja enviada

print("Producer finalizado.")

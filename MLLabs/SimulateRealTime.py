from kafka import KafkaProducer
import time
import subprocess

producer = KafkaProducer()

cat = subprocess.Popen(["hadoop", "fs", "-cat", "/data/*.txt"], stdout=subprocess.PIPE)
for line in cat.stdout:
    producer.send('realtime', line)
    time.sleep(2)
"""Sensores SIMULADOS del Anexo 3, con confirmación de entrega a Kafka."""
import argparse,json,random,time
from pathlib import Path
from kafka import KafkaProducer

def main():
 p=argparse.ArgumentParser();p.add_argument('--topic',default='sensor_data');p.add_argument('--eventos',type=int,default=120);p.add_argument('--intervalo',type=float,default=1);p.add_argument('--salida',default='resultados/streaming/productor.jsonl');a=p.parse_args()
 out=Path(a.salida);out.parent.mkdir(parents=True,exist_ok=True);rng=random.Random(106)
 producer=KafkaProducer(bootstrap_servers=['localhost:9092'],acks='all',retries=5,value_serializer=lambda x:json.dumps(x).encode('utf-8'))
 try:
  with out.open('w') as f:
   for i in range(a.eventos):
    event={'event_id':i+1,'sensor_id':1+i%10,'temperature':round(rng.uniform(20,30),2),'humidity':round(rng.uniform(30,70),2),'timestamp':int(time.time()),'simulado':True}
    meta=producer.send(a.topic,event).get(timeout=30);event['kafka_offset']=meta.offset
    f.write(json.dumps(event)+'\n');f.flush();print('ENVIADO '+json.dumps(event),flush=True);time.sleep(a.intervalo)
 finally:producer.flush();producer.close()
if __name__=='__main__':main()

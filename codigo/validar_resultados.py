"""Comprobación independiente con Decimal de los resultados obtenidos en Spark."""
import csv,json,argparse
from pathlib import Path
from decimal import Decimal
from datetime import date,timedelta,datetime,timezone
from collections import defaultdict

def main():
 p=argparse.ArgumentParser();p.add_argument('--base',default='.');a=p.parse_args();base=Path(a.base)
 with (base/'datos/trm_original.csv').open() as f:raw=list(csv.DictReader(f))
 s=json.loads((base/'resultados/batch/resumen.json').read_text());cut=date.fromisoformat(s['fecha_corte']);by_day={};seen=set()
 for r in raw:
  try:v=Decimal(r['valor']);start=date.fromisoformat(r['vigenciadesde'][:10]);end=date.fromisoformat(r['vigenciahasta'][:10])
  except Exception:continue
  if v<=0 or end<start or r['unidad']!='COP':continue
  key=(v,start,end,r['unidad'])
  if key in seen:continue
  seen.add(key)
  for i in range(max(0,(min(end,cut)-start).days+1)):
   day=start+timedelta(days=i);assert day not in by_day,'Solapamiento';by_day[day]=v
 grouped=defaultdict(list)
 for day,v in by_day.items():grouped[day.strftime('%Y-%m')].append(v)
 with (base/'resultados/batch/resumen_mensual.csv').open() as f:
  rows=list(csv.DictReader(f))
 for r in rows:
  vals=grouped[r['mes']];assert int(r['dias'])==len(vals);assert abs(sum(vals)/len(vals)-Decimal(r['promedio']))<Decimal('.000001')
 assert s['registros_originales']==len(raw) and s['dias']==len(by_day)
 checks={'batch_conteo_original_correcto':True,'batch_dias_correctos':True,'batch_promedios_mensuales_verificados':len(rows)}
 pf=base/'resultados/streaming/productor.jsonl'
 if pf.exists():
  events=[json.loads(l) for l in pf.read_text().splitlines()];expected=defaultdict(list)
  for e in events:
   key=(datetime.fromtimestamp(e['timestamp']//60*60,timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),str(e['sensor_id']));expected[key].append(e)
  actual={}
  for f in sorted((base/'resultados/streaming').glob('batch_*.csv')):
   with f.open() as inp:
    for r in csv.DictReader(inp):actual[(r['inicio_utc'],r['sensor_id'])]=r
  assert set(actual)==set(expected),(len(actual),len(expected))
  for k,es in expected.items():
   r=actual[k];assert int(r['eventos'])==len(es)
   for field,avg in [('temperature','temperatura_media'),('humidity','humedad_media')]:assert abs(sum(e[field] for e in es)/len(es)-float(r[avg]))<1e-9
  checks.update(streaming_eventos_confirmados=len(events),streaming_eventos_agregados=sum(int(r['eventos']) for r in actual.values()),streaming_ventanas_sensores_verificados=len(actual),streaming_promedios_correctos=True)
 (base/'resultados/validacion.json').write_text(json.dumps(checks,indent=2));print(json.dumps(checks,indent=2))
if __name__=='__main__':main()

"""Descarga oficial y reproducible de la TRM. Sin credenciales ni tokens."""
import csv, hashlib, io, json, urllib.request, urllib.parse
from pathlib import Path
from datetime import datetime, timezone
import argparse

def get(url):
    req=urllib.request.Request(url, headers={'User-Agent':'UNAD-Tarea3-TRM/1.0'})
    with urllib.request.urlopen(req, timeout=90) as r: return r.read()

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--salida',default='datos'); a=ap.parse_args()
    out=Path(a.salida);out.mkdir(parents=True,exist_ok=True)
    base='https://www.datos.gov.co/resource/mcec-87by.json'
    count=json.loads(get(base+'?'+urllib.parse.urlencode({'$select':'count(*)'})))[0]['count']
    rows=[]; urls=[]
    for offset in range(0,int(count),10000):
        url=base+'?'+urllib.parse.urlencode({'$limit':10000,'$offset':offset,'$order':'vigenciadesde ASC, vigenciahasta ASC, valor ASC'})
        rows.extend(json.loads(get(url)));urls.append(url)
    assert len(rows)==int(count), 'El origen cambió durante la descarga; vuelva a ejecutar.'
    raw=json.dumps(rows,ensure_ascii=False,indent=2).encode();(out/'trm_original.json').write_bytes(raw)
    fields=['valor','unidad','vigenciadesde','vigenciahasta']
    with (out/'trm_original.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
    meta=json.loads(get('https://www.datos.gov.co/api/views/mcec-87by.json'))
    provenance={'fuente':'https://www.datos.gov.co/d/mcec-87by','api':base,'consultas':urls,'consulta_utc':datetime.now(timezone.utc).isoformat(),'titulo':meta.get('name'),'descripcion':meta.get('description'),'atribucion':meta.get('attribution'),'registros':len(rows),'sha256_csv':hashlib.sha256((out/'trm_original.csv').read_bytes()).hexdigest(),'bytes_csv':(out/'trm_original.csv').stat().st_size,'columnas':fields}
    (out/'procedencia.json').write_text(json.dumps(provenance,ensure_ascii=False,indent=2));print(json.dumps(provenance,ensure_ascii=False,indent=2))
if __name__=='__main__': main()

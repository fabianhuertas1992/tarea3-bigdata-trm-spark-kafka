"""Gráficos a partir de las salidas calculadas por Spark, sin alterar resultados."""
import csv,json,argparse
from pathlib import Path
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def leer(p):
 with p.open() as f:return list(csv.DictReader(f))
def main():
 p=argparse.ArgumentParser();p.add_argument('--base',default='.');a=p.parse_args();base=Path(a.base);out=base/'resultados/graficos';out.mkdir(parents=True,exist_ok=True)
 plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.spines.top':False,'axes.spines.right':False,'figure.dpi':140})
 m=leer(base/'resultados/batch/resumen_mensual.csv');d=leer(base/'resultados/batch/serie_diaria.csv')
 fig,ax=plt.subplots(figsize=(10,4.5));ax.plot([datetime.fromisoformat(x['mes']+'-01') for x in m],[float(x['promedio']) for x in m],color='#155e75',lw=1.8);ax.set(title='TRM promedio mensual por día calendario',ylabel='Pesos colombianos por dólar',xlabel='Año');ax.grid(alpha=.2);fig.tight_layout();fig.savefig(out/'01_trm_historica.png');plt.close(fig)
 recent=[x for x in m if x['mes']>='2025-01'];fig,ax=plt.subplots(figsize=(10,4.5));ax.bar([x['mes'] for x in recent],[float(x['promedio']) for x in recent],color='#155e75');ax.tick_params(axis='x',rotation=60);ax.set(title='Promedios mensuales de 2025 y 2026',ylabel='COP por USD');ax.text(.01,.98,'Octubre de 2026 tiene corte al día 25',transform=ax.transAxes,va='top',fontsize=9);fig.tight_layout();fig.savefig(out/'02_trm_reciente.png');plt.close(fig)
 # El gráfico presenta la última actualización de cada ventana y sensor.
 latest={}
 for f in sorted((base/'resultados/streaming').glob('batch_*.csv')):
  for r in leer(f):latest[(r['inicio_utc'],r['sensor_id'])]=r
 if latest:
  rows=sorted(latest.values(),key=lambda r:(r['inicio_utc'],int(r['sensor_id'])))
  with (base/'resultados/streaming/ventanas_finales.csv').open('w',newline='') as f:
   w=csv.DictWriter(f,fieldnames=rows[0].keys());w.writeheader();w.writerows(rows)
  fig,ax=plt.subplots(figsize=(10,4.5))
  for sid in sorted({int(r['sensor_id']) for r in rows}):
   group=[r for r in rows if int(r['sensor_id'])==sid];ax.plot([r['inicio_utc'][11:16] for r in group],[float(r['temperatura_media']) for r in group],marker='o',label=f'Sensor {sid}')
  ax.set(title='Temperatura simulada por ventana de un minuto',xlabel='Inicio de ventana UTC',ylabel='Temperatura media °C');ax.legend(ncol=5,fontsize=8,loc='upper center',bbox_to_anchor=(.5,-.18));ax.grid(alpha=.2);fig.tight_layout();fig.savefig(out/'03_streaming_sensores.png');plt.close(fig)
 print('Gráficos guardados en',out)
if __name__=='__main__':main()

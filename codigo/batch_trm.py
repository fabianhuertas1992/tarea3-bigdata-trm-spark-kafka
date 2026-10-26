"""Análisis batch TRM: cotizaciones originales y promedio por día calendario."""
import argparse,csv,json
from pathlib import Path
from datetime import datetime,timezone
from pyspark.sql import SparkSession,functions as F,types as T,Window

def main():
    p=argparse.ArgumentParser();p.add_argument('--entrada',required=True);p.add_argument('--salida',default='resultados/batch');p.add_argument('--fecha-corte',default=datetime.now().date().isoformat());a=p.parse_args()
    out=Path(a.salida);out.mkdir(parents=True,exist_ok=True)
    spark=SparkSession.builder.appName('Tarea3_TRM_Batch').config('spark.sql.session.timeZone','America/Bogota').config('spark.sql.shuffle.partitions','2').getOrCreate();spark.sparkContext.setLogLevel('WARN')
    schema=T.StructType([T.StructField(x,T.StringType(),True) for x in ['valor','unidad','vigenciadesde','vigenciahasta']])
    raw=spark.read.option('header',True).schema(schema).csv(a.entrada).cache();n=raw.count()
    typed=raw.select(F.col('valor').cast('decimal(14,2)').alias('trm'),F.col('unidad'),F.to_date('vigenciadesde').alias('desde'),F.to_date('vigenciahasta').alias('hasta'))
    valid=(F.col('trm').isNotNull()&(F.col('trm')>0)&F.col('desde').isNotNull()&F.col('hasta').isNotNull()&(F.col('hasta')>=F.col('desde'))&(F.col('unidad')=='COP'))
    invalid=typed.filter(~F.coalesce(valid,F.lit(False)));clean=typed.filter(valid).dropDuplicates(['trm','unidad','desde','hasta']).cache()
    valid_n=typed.filter(valid).count();unique_n=clean.count()
    # Se limita al día de consulta, aunque una vigencia se extienda al día siguiente.
    eligible=clean.filter(F.col('desde')<=F.lit(a.fecha_corte).cast('date'))
    daily=eligible.withColumn('fin',F.least(F.col('hasta'),F.lit(a.fecha_corte).cast('date'))).select('trm',F.explode(F.sequence('desde','fin')).alias('fecha')).cache()
    overlap=daily.groupBy('fecha').count().filter('count > 1').count()
    if overlap: raise RuntimeError('Vigencias superpuestas: revisar antes de calcular promedios diarios.')
    daily.createOrReplaceTempView('trm_diaria')
    monthly=spark.sql('SELECT date_format(fecha, "yyyy-MM") AS mes, count(*) AS dias, avg(trm) AS promedio, min(trm) AS minimo, max(trm) AS maximo, stddev_samp(cast(trm as double)) AS desviacion FROM trm_diaria GROUP BY date_format(fecha, "yyyy-MM") ORDER BY mes')
    changes=eligible.withColumn('anterior',F.lag('trm').over(Window.orderBy('desde','hasta'))).withColumn('cambio_cop',F.col('trm')-F.col('anterior')).withColumn('cambio_pct',100*(F.col('trm')/F.col('anterior')-1))
    def save(df,name):
        rows=[r.asDict(recursive=True) for r in df.collect()]
        with (out/(name+'.csv')).open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=df.columns);w.writeheader();w.writerows(rows)
        return rows
    save(clean.orderBy('desde'),'cotizaciones_limpias');save(invalid,'filas_invalidas');save(daily.orderBy('fecha'),'serie_diaria');save(monthly,'resumen_mensual')
    top=save(changes.filter('anterior is not null').orderBy(F.abs(F.col('cambio_pct')).desc()).limit(10),'mayores_variaciones')
    annual=spark.sql('SELECT year(fecha) AS anio, count(*) AS dias, avg(trm) AS promedio, min(trm) AS minimo, max(trm) AS maximo FROM trm_diaria GROUP BY year(fecha) ORDER BY anio');save(annual,'resumen_anual')
    rdd_counts=dict(daily.rdd.map(lambda r:(r.fecha.year,1)).reduceByKey(lambda a,b:a+b).collect())
    assert rdd_counts=={r.anio:r.dias for r in annual.collect()}
    stats=daily.agg(F.count('*').alias('dias'),F.min('fecha').alias('inicio'),F.max('fecha').alias('fin'),F.min('trm').alias('minimo'),F.max('trm').alias('maximo'),F.avg('trm').alias('promedio')).first().asDict()
    expected_days=(stats['fin']-stats['inicio']).days+1
    summary={'spark':spark.version,'master':spark.sparkContext.master,'entrada':a.entrada,'fecha_corte':a.fecha_corte,'ejecucion_utc':datetime.now(timezone.utc).isoformat(),'registros_originales':n,'invalidos':invalid.count(),'duplicados_exactos':valid_n-unique_n,'cotizaciones_limpias':unique_n,'cotizaciones_posteriores_al_corte':unique_n-eligible.count(),'fechas_superpuestas':overlap,'dias_sin_cotizacion':expected_days-stats['dias'],'comprobacion_RDD_DataFrame':True,**stats,'mayor_variacion_pct':top[0] if top else None}
    (out/'resumen.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2,default=str))
    print('RESUMEN VALIDADO\n'+json.dumps(summary,ensure_ascii=False,indent=2,default=str));monthly.orderBy(F.desc('mes')).show(12,False);changes.orderBy(F.abs('cambio_pct').desc()).show(5,False)
    spark.stop()
if __name__=='__main__':main()

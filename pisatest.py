from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyarrow.parquet as pq
import duckdb
import pandas as pd
spark = SparkSession.builder \
    .appName("Lectura de archivo CSV") \
    .getOrCreate()

stu_cog = spark.read.csv("STU_COG.csv", header=True, inferSchema=True)
stu_cog_per = stu_cog.filter(col("CNT") == "PER")
stu_cog_per.write.parquet("stucog_per.parquet")


sch = spark.read.csv("SCH.csv", header=True, inferSchema=True)
sch_per = sch.filter(col("CNT") == "PER")
sch_per.write.parquet("sch_per.parquet")

tch = spark.read.csv("TCH.csv", header=True, inferSchema=True)
tch_per = tch.filter(col("CNT") == "PER")
tch_per.write.parquet("tch_per.parquet")

stuqqq = spark.read.csv("STU_QQQ.csv", header=True, inferSchema=True)
stuqqq_per = stuqqq.filter(col("CNT") == "PER")
stuqqq_per.write.parquet("stuqqq_per.parquet")

stutim = spark.read.csv("STU_TIM.csv", header=True, inferSchema=True)
stutim_per = stutim.filter(col("CNT") == "PER")
stutim_per.write.parquet("stutim_per.parquet")

spark.stop()

stu_df =  pq.read_table('stuqqq_per.parquet').to_pandas()
sch_df =  pq.read_table('sch_per.parquet').to_pandas()


# tipo de colegio y género del estudiante

gen_tipocol = duckdb.sql(""" SELECT 
    CASE WHEN stu_df.ST004D01T = 1 THEN 'Femenino'
         WHEN stu_df.ST004D01T = 2 THEN 'Masculino'
         ELSE NULL END AS 'Genero',
    CASE WHEN sch_df.SC013Q01TA = 1 THEN 'Publico'
         WHEN sch_df.SC013Q01TA = 2 THEN 'Privado'
         ELSE NULL END AS 'Tipo_Colegio'
FROM 
    stu_df
JOIN
    sch_df
ON
    stu_df.CNTSCHID = sch_df.CNTSCHID;""") 

# Notas de matemáticas, lectura y ciencias

todo_notas = duckdb.sql("""SELECT 
    Score_Matematicas,
    Score_Lectura,
    Score_Ciencias,
    (Score_Matematicas + Score_Lectura + Score_Ciencias) / 3 AS 'Score_Total'
FROM (
    SELECT 
        (PV1MATH + PV2MATH + PV3MATH + PV4MATH + PV5MATH + PV6MATH + PV7MATH + PV8MATH + PV9MATH + PV10MATH) / 10 AS 'Score_Matematicas',
        (PV1READ + PV2READ + PV3READ + PV4READ + PV5READ + PV6READ + PV7READ + PV8READ + PV9READ + PV10READ) / 10 AS 'Score_Lectura',
        (PV1SCIE + PV2SCIE + PV3SCIE + PV4SCIE + PV5SCIE + PV6SCIE + PV7SCIE + PV8SCIE + PV9SCIE + PV10SCIE) / 10 AS 'Score_Ciencias'
    FROM 
        stu_df
) AS subconsulta;""") 

# El estudiante come 3 veces al día

comenocome = duckdb.sql("""SELECT     
    CASE 
        WHEN stu_df.ST258Q01JA = '1' THEN 'Si'
        WHEN stu_df.ST258Q01JA IN ('2', '3', '4', '5') THEN 'No'
        ELSE stu_df.ST258Q01JA
    END AS '3_Comidas_por_día'
FROM 
    stu_df;""")


# Existe al menos un padre en casa con estudios superiores
papas_con_estudios = duckdb.sql("""
SELECT  
    CASE
        WHEN Padre_con_estudios_superiores = 'Si' OR Madre_con_estudios_superiores = 'Si' THEN 'Si'
        WHEN Padre_con_estudios_superiores = 'No' AND Madre_con_estudios_superiores = 'No' THEN 'No'
        ELSE NULL
    END AS 'Apoderado_con_estudios_superiores'
FROM (
    SELECT     
        CASE 
            WHEN FISCED IN ('8', '9', '10') THEN 'Si'
            WHEN FISCED IN ('1', '2', '3', '4', '5', '6', '7') THEN 'No'
            ELSE NULL
        END AS 'Padre_con_estudios_superiores',
        CASE 
            WHEN MISCED IN ('8', '9', '10') THEN 'Si'
            WHEN MISCED IN ('1', '2', '3', '4', '5', '6', '7') THEN 'No'
            ELSE NULL
        END AS 'Madre_con_estudios_superiores',
        stu_df.ST258Q01JA
    FROM 
        stu_df
) """)

# Hay internet en la casa
internet = duckdb.sql("""SELECT
        CASE
            WHEN ST250Q04JA = '1' OR ST250Q05JA = '1' THEN 'Si'
            WHEN ST250Q04JA = '2' AND ST250Q05JA = '2' THEN 'No'
            ELSE NULL
        END AS Internet
    FROM
        stu_df""")

# Se resuelven tareas o se estudia diariamente
estudio_diario = duckdb.sql("""SELECT     
    CASE 
        WHEN STUDYHMW IN (5, 6, 7, 8 ,9, 10) THEN 'Si'
        WHEN STUDYHMW IN (2, 3, 4, 1 , 0) THEN 'No'
        ELSE NULL
    END AS 'estudio_diario'
FROM 
    stu_df;""")
           


# Existen evaluaciones continuas (mayores a 1 vez al mes)
evaluacion_continua = duckdb.sql(""" SELECT
        CASE 
            WHEN SC195Q03JA = 5 THEN 'Si'
            WHEN SC195Q03JA IN (1, 2, 3, 4) THEN 'No'
            ELSE NULL
        END AS Eval_cont,
        FROM
        sch_df
    JOIN
    stu_df
ON
    stu_df.CNTSCHID = sch_df.CNTSCHID;""") 

# Se incluye la evaluación emocional y social como parte de la curricula
psi_evaluacion = duckdb.sql("""SELECT 
           CASE 
            WHEN SC189Q06JA = 1 THEN 'Si'
            WHEN SC189Q06JA = 2 THEN 'No'
            ELSE NULL
        END AS Psi_evaluacion,
        FROM
        sch_df 
        JOIN
    stu_df
ON
    stu_df.CNTSCHID = sch_df.CNTSCHID;""")

#La escuela le da recursos a los padres para la enseñanza de sus hijos en casa?
esc_padres = duckdb.sql("""SELECT 
           CASE 
            WHEN SC223Q07JA IN (1,2) THEN 'Si'
            WHEN SC223Q07JA = 3 THEN 'No'
            ELSE NULL
        END AS asesoria_parental,
        FROM
        sch_df 
        JOIN
    stu_df
ON
    stu_df.CNTSCHID = sch_df.CNTSCHID;""")

# Se pueden usar dispsitivos digitales en la escuela
uso_dig = duckdb.sql("""SELECT 
           CASE 
            WHEN SC190Q05JA = 1 THEN 'Si'
            WHEN SC190Q05JA = 2 THEN 'No'
            ELSE NULL
        END AS uso_digital_escuela,
        FROM
        sch_df 
        JOIN
    stu_df
ON
    stu_df.CNTSCHID = sch_df.CNTSCHID;""")


pisa_res = pd.concat([gen_tipocol.df(),todo_notas.df(), 
           comenocome.df(), papas_con_estudios.df(), internet.df(), estudio_diario.df(),
           evaluacion_continua.df(), psi_evaluacion.df() ,uso_dig.df(), esc_padres.df()], axis=1)
pisa_res.to_csv('pisa_res.csv', index=False)
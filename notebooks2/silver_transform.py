from pyspark.sql import functions as F

# Limpeza das informações que continham nome Valor
df_clean = df_bronze.filter(F.col("V") != "Valor")
display(df_clean)

# Renomeando colunas
df_clean = (df_clean
            .withColumnRenamed("D1C", "un_federacao_codigo")
            .withColumnRenamed("D1N", "un_federacao_nome")
            .withColumnRenamed("D2C", "ano_codigo")
            .withColumnRenamed("D2N", "ano")
            .withColumnRenamed("D3C", "variavel_codigo")
            .withColumnRenamed("D3N", "variavel")
            .withColumnRenamed("MC", "un_medida_codigo")
            .withColumnRenamed("MN", "un_medida")
            .withColumnRenamed("NC", "nivel_territorial_codigo")
            .withColumnRenamed("NN", "nivel_territorial")
            .withColumnRenamed("V", "valor")
            )
display(df_clean)

# Transformando a coluna valor em double 
df_clean = df_clean.withColumn(
    "valor",
    F.regexp_replace(F.col("valor"), ",", ".").cast("double")
)
display(df_clean)

# Validação da coluna valor
df_clean = df_clean.withColumn(
    "valor_valido",
    F.col("valor").isNotNull()
)
display(df_clean)

Criação da tabela silver
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

(
    df_clean
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("silver.sidra_6579_clean")
)

# Verificação da tabela
%sql
SELECT *
FROM silver.sidra_6579_clean
LIMIT 10;

# Validando as informações da coluna valor para a camada Gold
df_valid = df_silver.filter(F.col("valor_valido") == True)
display(df_valid)

# Agregando as informações ano, un_federacao_nome, un_federacao_codigo, "nivel_territorial"
df_gold = (
    df_valid
        .groupBy("ano", "un_federacao_nome", "un_federacao_codigo", "nivel_territorial")
        .agg(
            F.sum("valor").alias("valor_total"),
            F.count("*").alias("qtd_registros")
        )
)
display(df_gold)

# Criação na camada Gold
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

(
    df_gold
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("gold.sidra_6579_agg")
)

# Visualização das informações
SELECT *
FROM gold.sidra_6579_agg
ORDER BY ano DESC, un_federacao_nome 
LIMIT 20;

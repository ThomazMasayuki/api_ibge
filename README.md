# Pipeline de Dados - IBGE SIDRA (Tabela 6579)
Criação de processo de Engenharia de Dados utilizando a arquitetura medalhão (Bronze, Silver, Gold)

## 1. Descrição da Solução
Projeto realizado para implementar um pipeline de dados para consumo analítico a partir de dados disponibilizados via API do IBGE SIDRA (tabela 6579).

A solução foi estruturada seguindo o padrão de arquitetura medalhão, separando as responsabilidades entre as camadas Bronze, Silver e Gold, tendo o foco em:

- Confiabilidade da ingestão;
- Rastreabilidade;
- Qualidade dos dados;
- Preparação para consumo analítico.

A implementação foi realizada no Databricks Free Edition, utilizando Pyspark e Delta Lake.

## 2. Fonte de dados
- Origem: API pública do IBGE - SIDRA
- Endpoint: https://apisidra.ibge.gov.br/values/t/6579/n3/all/p/all/v/all
- Tipo: API Rest (JSON)
- Periodicidade: Dados públicos, atualizados conforme publicação do IBGE

## 3. Arquitetura adotada (Medalhão)
O pipeline foi organizado da seguinte forma:

**API IBGE SIDRA -> Bronze -> Silver -> Gold**

## 4. Camadas do pipeline

**Camada Bronze**

**Objetivo:** Persistir os dados como recebidos da API, garantindo histórico e rastreabilidade.

Características da etapa:
- Ingestão direta da API do IBGE;
- Dados mantidos sem transformação semântica;
- Tipos preservados como string;
- Inclusão de metadados técnicos para governança;
- Persistência em formato Delta Lake.

**Metadados adicionados:**
- _ingestion_ts: timestamp da ingestão
- _load_date: data da carga
- _source_url: endpoint de origem dos dados

**- Tabela criada: bronze.sidra_6579_raw**

**Camada Silver**

**Objetivo:** Preparar os dados para uso analítico, aplicando regras técnicas e qualidade básica.

Características da etapa:

- Remoção de registros inválidos (ex: linha de cabeçalho da API);
- Renomeação de colunas técnicas para nomes semânticos;
- Conversão de tipos (string -> numérico);
- Criação de flags de qualidade (ex: valor válido).

**- Tabela criada: silver.sidra_6579_clean**

**Camada Gold**

**Objetivo: Disponibilizar dados agregados, prontos para consumo**

Características da etapa:

- Leitura da camada Silver;
- Filtragem de registros válidos;
- Agregações analíticas (ex: total por ano, unidade/código de federação, nivel territorial);
- Redução de volume e orientação a consumo.

**Tabela criada: gold.sidra_6579_agg**

## 5. Instruções básicas de execução

A execução do pipeline deve seguir a ordem abaixo, no Databricks:

1. Executar o notebook/script de ingestão (Bronze)
2. Executar o notebook/script de tratamento (Silver)
3. Executar o notebook/script de agregação (Gold)

**Obs: Cada etapa depende exclusivamente da camada anterior.**

## 6. Explicação das decisões adotadas

- Porque usar na etapa de fonte e ingestão de dados as funções "get_json_with_retries" e "run"?

A criação da função foi pensada em escalabilidade, pois, em projetos de engenharia de dados, a ideia de uso e consumo de dados é sempre em escala, sendo necessário entender o uso da parte final e criar as etapas com base nas regras de negócio.

- Porque usar o método de validação na coluna "valor"?

É importante validar valores durante o processo da camada Silver, isso trás confiabilidade nos dados tanto para quem está criando a base quanto para quem vai utilizar as informações.

- Porque juntar as colunas "ano", "un_federacao_nome", ""un_federacao_codigo" e "nivel_territorial"?

Por se tratar de informações sobre nível territorial do Brasil, ao agregar essas informações é possível do usuário final criar visualizações ou analisar dados sobre o valor total em cada estado.
   
## 7. Evidências da arquitetura medalhão

- A arquiterura medalhão é evidenciada por meio da separação em schemas e tabelas Delta, conforme as imagens 1 e 2: 

Imagem 1:
<img width="1044" height="476" alt="Captura de Tela 2026-02-01 às 20 49 16" src="https://github.com/user-attachments/assets/e1235ad4-7bea-49be-83db-00e796f35cf7" />

Imagem 2:
<img width="294" height="330" alt="Captura de Tela 2026-02-01 às 20 57 40" src="https://github.com/user-attachments/assets/c68f3812-748c-4125-baf5-f6450f81e063" />

## 8. Considerações finais:

- A solução foi implementada para ser simples e escalável, podendo ser adaptada para ambientes com maior volume de dados.





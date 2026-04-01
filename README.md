# 🏦 Fintech AWS Data Pipeline

Pipeline de dados completo para área financeira construído na AWS, seguindo a **Arquitetura Medallion** (Bronze → Silver → Gold) com modelagem dimensional **Star Schema**.

> Projeto prático de aprendizado de AWS com foco em Engenharia de Dados.

---

## 📐 Arquitetura

```
Fonte (CSV)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│                    AWS S3                           │
│  /raw → /bronze → /silver → /gold                  │
└─────────────────────────────────────────────────────┘
    │           │           │           │
    ▼           ▼           ▼           ▼
 Upload      Lambda      Glue ETL    Glue ETL
  CSV       CSV→Parquet  Limpeza    Dimensões
                                   + Fato
                                       │
                                       ▼
                                   Amazon Athena
                                   (Queries SQL)
                                       │
                                       ▼
                                   Power BI
                                  (Dashboard)
```

---

## 🗂️ Arquitetura Medallion

| Camada | Pasta S3 | Descrição |
|---|---|---|
| **Raw** | `/raw` | CSVs originais carregados pelo script Python |
| **Bronze** | `/bronze` | Parquet bruto — dado como veio da fonte |
| **Silver** | `/silver` | Dado limpo, tipado e deduplicado |
| **Gold** | `/gold` | Modelo dimensional Star Schema para BI |

---

## ⭐ Modelo Dimensional (Star Schema)

```
                    dim_tempo
                       │
    dim_cliente ── fat_transacoes ── dim_cartao
                       │
              dim_comerciante
              dim_pagamento
              dim_status
              dim_localizacao
```

### Tabelas Gold

| Tabela | Tipo | Registros | Descrição |
|---|---|---|---|
| `fat_transacoes` | Fato | 3,386,122 | Transações de cartão |
| `dim_tempo` | Dimensão | 455 | Calendário Jan/2025–Mar/2026 |
| `dim_cliente` | Dimensão | 20,000 | Perfil dos clientes |
| `dim_cartao` | Dimensão | 20,000 | Dados do cartão e limite |
| `dim_comerciante` | Dimensão | 488 | Estabelecimentos |
| `dim_pagamento` | Dimensão | 26 | Tipos de pagamento |
| `dim_status` | Dimensão | 14 | Status e motivos de recusa |
| `dim_localizacao` | Dimensão | 20 | Regiões de Brasília/DF |

---

## 🛠️ Serviços AWS Utilizados

| Serviço | Uso |
|---|---|
| **Amazon S3** | Data Lake — armazenamento de todas as camadas |
| **AWS Lambda** | Ingestão automática CSV → Parquet |
| **AWS Glue Crawler** | Catalogação automática do schema |
| **AWS Glue ETL** | Transformação Bronze → Silver → Gold |
| **Amazon Athena** | Queries SQL serverless |
| **AWS IAM** | Controle de acesso e permissões |
| **Amazon CloudWatch** | Monitoramento e logs |
| **Power BI** | Dashboard e visualização |

---

## 📊 Dados Simulados

Dados financeiros realistas gerados com Python + Faker:

- **20.000 clientes** com endereços reais de Brasília/DF
- **20.000 perfis** com cartões Gold, Platinum e Black
- **3,3 milhões de transações** de Jan/2025 a Mar/2026
- Compras em estabelecimentos do mundo todo
- Múltiplas bandeiras: Visa, Mastercard, Elo, Amex

---

## 🚀 Como Executar

### Pré-requisitos

```bash
pip install boto3 pandas pyarrow faker
aws configure  # configure com suas credenciais IAM
```

### 1. Gerar os dados

```bash
cd 01_data_generation
python generate_bank_data.py
```

### 2. Configurar a Lambda

- Deploy do código em `02_ingestion/lambda_function.py`
- Adicionar Layer: `AWSSDKPandas-Python311`
- Configurar trigger S3 na pasta `/raw`
- Timeout: 15 min | Memory: 3008 MB | Storage: 2048 MB

### 3. Executar o ETL Bronze → Silver

```bash
# Via AWS Glue Console ou CLI
aws glue start-job-run --job-name bronze-to-silver-customers
aws glue start-job-run --job-name bronze-to-silver-customer-profiles
aws glue start-job-run --job-name bronze-to-silver-transactions
```

### 4. Executar o ETL Silver → Gold

```bash
# Via AWS Glue Notebook ou Script Editor
# Rodar os scripts em 04_silver_to_gold/ na ordem:
# 1. dim_tempo.py
# 2. dim_cliente.py
# 3. dim_localizacao.py
# 4. dim_cartao.py
# 5. dim_comerciante.py
# 6. dim_pagamento.py
# 7. dim_status.py
# 8. fat_transacoes.py
```

### 5. Catalogar no Athena

```bash
aws glue start-crawler --name fintech-crawler
```

---

## 🔒 Segurança

- ⚠️ **Nunca commite credenciais AWS** — use variáveis de ambiente
- Todas as Access Keys devem ser do usuário IAM, não da conta root
- O `.gitignore` já está configurado para ignorar arquivos sensíveis
- Em produção, use AWS Secrets Manager para gerenciar credenciais

---

## 📁 Estrutura do Repositório

```
fintech-aws-pipeline/
├── 01_data_generation/     # Geração de dados simulados
├── 02_ingestion/           # Lambda function
├── 03_bronze_to_silver/    # Jobs ETL de limpeza
├── 04_silver_to_gold/      # Jobs ETL de modelagem
├── 05_queries/             # Queries SQL de validação e análise
└── docs/                   # Diagramas e documentação
```

---

## 🗺️ Roadmap

- [x] Geração de dados simulados
- [x] Pipeline de ingestão com Lambda
- [x] Arquitetura Medallion (Bronze/Silver/Gold)
- [x] Modelo dimensional Star Schema
- [x] Queries SQL com Amazon Athena
- [x] Dashboard com Power BI
- [ ] Automação end-to-end com Step Functions
- [ ] Ingestão via SFTP com AWS Transfer Family
- [ ] Infrastructure as Code com Terraform
- [ ] Testes de qualidade de dados com Great Expectations
- [ ] CI/CD com GitHub Actions

---

## 👨‍💻 Autor - André Souza 


Desenvolvido como projeto prático de aprendizado de AWS Data Engineering.

**Brasília/DF — 2026**

---

## 📄 Licença

MIT License — sinta-se livre para usar e adaptar.

"""
=============================================================
  Simulador de Dados Bancários — Brasília/DF
  Gera 3 arquivos CSV e faz upload para S3:
    - customers.csv         (20.000 clientes)
    - customer_profiles.csv (20.000 perfis)
    - transactions.csv      (~200.000 transações)
=============================================================
  Dependências: pip install boto3 pandas faker pyarrow
=============================================================
"""

import uuid
import random
import boto3
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta, date

# ── Configurações ──────────────────────────────────────────────────────────────
BUCKET_NAME    = "fintech-pipeline-raw-263704"   # ← troque pelo seu bucket
S3_PREFIX      = "raw/"
REGION         = "us-east-1"
NUM_CUSTOMERS  = 20_000
START_DATE     = date(2025, 1, 1)
END_DATE       = date(2026, 3, 31)
RANDOM_SEED    = 42

random.seed(RANDOM_SEED)
fake = Faker("pt_BR")
Faker.seed(RANDOM_SEED)

print("=" * 60)
print("  Simulador de Dados Bancários — iniciando")
print("=" * 60)


# ══════════════════════════════════════════════════════════════
#  DADOS DE REFERÊNCIA
# ══════════════════════════════════════════════════════════════

# Regiões Administrativas de Brasília com CEPs e bairros reais
BRASILIA_REGIONS = [
    {"neighborhood": "Asa Norte",          "zip_prefix": "70750", "weight": 8},
    {"neighborhood": "Asa Sul",            "zip_prefix": "70390", "weight": 8},
    {"neighborhood": "Lago Sul",           "zip_prefix": "71600", "weight": 4},
    {"neighborhood": "Lago Norte",         "zip_prefix": "71500", "weight": 4},
    {"neighborhood": "Sudoeste",           "zip_prefix": "70675", "weight": 5},
    {"neighborhood": "Noroeste",           "zip_prefix": "70855", "weight": 4},
    {"neighborhood": "Taguatinga",         "zip_prefix": "72010", "weight": 7},
    {"neighborhood": "Ceilândia",          "zip_prefix": "72210", "weight": 9},
    {"neighborhood": "Samambaia",          "zip_prefix": "72300", "weight": 6},
    {"neighborhood": "Gama",               "zip_prefix": "72405", "weight": 5},
    {"neighborhood": "Sobradinho",         "zip_prefix": "73040", "weight": 4},
    {"neighborhood": "Planaltina",         "zip_prefix": "73300", "weight": 4},
    {"neighborhood": "Águas Claras",       "zip_prefix": "71901", "weight": 6},
    {"neighborhood": "Vicente Pires",      "zip_prefix": "72006", "weight": 3},
    {"neighborhood": "Guará",              "zip_prefix": "71010", "weight": 5},
    {"neighborhood": "Riacho Fundo",       "zip_prefix": "71820", "weight": 3},
    {"neighborhood": "Santa Maria",        "zip_prefix": "72500", "weight": 5},
    {"neighborhood": "São Sebastião",      "zip_prefix": "71690", "weight": 3},
    {"neighborhood": "Recanto das Emas",   "zip_prefix": "72600", "weight": 4},
    {"neighborhood": "Park Way",           "zip_prefix": "71740", "weight": 2},
]

STREET_TYPES = [
    "SQN", "SQS", "CLN", "CLS", "SHIN", "SHIS", "QI", "QD",
    "EQN", "EQS", "SMPW", "SMU", "Rua", "Quadra", "Setor"
]

INCOME_RANGES = [
    {"range": "up_to_1k",   "min": 500,    "max": 1_000,   "weight": 10},
    {"range": "1k_to_2k",   "min": 1_000,  "max": 2_000,   "weight": 15},
    {"range": "2k_to_3k",   "min": 2_000,  "max": 3_000,   "weight": 15},
    {"range": "3k_to_5k",   "min": 3_000,  "max": 5_000,   "weight": 20},
    {"range": "5k_to_10k",  "min": 5_000,  "max": 10_000,  "weight": 18},
    {"range": "10k_to_20k", "min": 10_000, "max": 20_000,  "weight": 12},
    {"range": "20k_to_50k", "min": 20_000, "max": 50_000,  "weight": 7},
    {"range": "above_50k",  "min": 50_000, "max": 200_000, "weight": 3},
]

OCCUPATIONS = [
    ("Servidor Público Federal", 0.20),
    ("Servidor Público Distrital", 0.12),
    ("Militar", 0.06),
    ("Empresário", 0.08),
    ("Médico", 0.04),
    ("Advogado", 0.04),
    ("Engenheiro", 0.05),
    ("Professor", 0.06),
    ("Comerciante", 0.05),
    ("Autônomo", 0.08),
    ("Aposentado", 0.07),
    ("Estudante", 0.06),
    ("Analista de TI", 0.04),
    ("Outros", 0.05),
]

CARD_TIERS = [
    {"tier": "gold",     "limit_min": 1_000,  "limit_max": 8_000,  "weight": 50},
    {"tier": "platinum", "limit_min": 8_000,  "limit_max": 30_000, "weight": 35},
    {"tier": "black",    "limit_min": 30_000, "limit_max": 100_000,"weight": 15},
]

CARD_FLAGS = [
    ("Visa",       0.38),
    ("Mastercard", 0.35),
    ("Elo",        0.18),
    ("Amex",       0.09),
]

ACCOUNT_TYPES = [
    ("checking",    0.55),
    ("savings",     0.25),
    ("investment",  0.20),
]

# Categorias MCC realistas com estabelecimentos e países
MERCHANT_CATEGORIES = [
    {
        "category": "restaurants_food",
        "merchants": ["McDonald's", "Burger King", "Outback", "iFood", "Subway",
                      "Bob's", "Pizza Hut", "Domino's", "Giraffas", "Spoleto"],
        "countries": [("BR", 0.92), ("US", 0.04), ("AR", 0.02), ("PT", 0.02)],
        "amount_min": 20,  "amount_max": 350,  "weight": 18,
    },
    {
        "category": "supermarkets_grocery",
        "merchants": ["Carrefour", "Extra", "Pão de Açúcar", "Walmart", "Atacadão",
                      "BIG", "Comper", "Assaí", "Rede Smart", "Supermercado Nagumo"],
        "countries": [("BR", 0.98), ("US", 0.01), ("FR", 0.01)],
        "amount_min": 50,  "amount_max": 1_200, "weight": 15,
    },
    {
        "category": "fuel_automotive",
        "merchants": ["Posto Shell", "Posto Ipiranga", "Posto BR", "Posto Ale",
                      "Auto Posto Estrela", "Raizen", "Vibra Energia"],
        "countries": [("BR", 0.97), ("US", 0.02), ("AR", 0.01)],
        "amount_min": 80,  "amount_max": 600,  "weight": 10,
    },
    {
        "category": "health_pharmacy",
        "merchants": ["Drogasil", "Droga Raia", "Ultrafarma", "Farmácia Popular",
                      "Onofre", "Pacheco", "São João Farmácias", "Drogaria Araújo"],
        "countries": [("BR", 0.99), ("US", 0.01)],
        "amount_min": 15,  "amount_max": 800,  "weight": 9,
    },
    {
        "category": "e_commerce",
        "merchants": ["Amazon", "Mercado Livre", "Americanas", "Shopee", "Magazine Luiza",
                      "Casas Bahia", "Submarino", "Netshoes", "Centauro", "Ali Express"],
        "countries": [("BR", 0.80), ("US", 0.12), ("CN", 0.06), ("DE", 0.02)],
        "amount_min": 30,  "amount_max": 5_000, "weight": 14,
    },
    {
        "category": "travel_airlines",
        "merchants": ["LATAM Airlines", "Gol Linhas Aéreas", "Azul", "TAP Air Portugal",
                      "American Airlines", "Emirates", "Air France", "Iberia", "Copa Airlines"],
        "countries": [("BR", 0.55), ("US", 0.15), ("PT", 0.08), ("FR", 0.06),
                      ("AE", 0.05), ("ES", 0.05), ("CO", 0.03), ("AR", 0.03)],
        "amount_min": 300, "amount_max": 12_000,"weight": 5,
    },
    {
        "category": "hotels_accommodation",
        "merchants": ["Marriott", "Hilton", "Ibis", "Accor Hotels", "Booking.com",
                      "Airbnb", "Windsor Hotels", "Nacional Inn", "Blue Tree", "Intercity"],
        "countries": [("BR", 0.60), ("US", 0.15), ("FR", 0.07), ("ES", 0.06),
                      ("IT", 0.04), ("PT", 0.04), ("AR", 0.04)],
        "amount_min": 150, "amount_max": 8_000, "weight": 4,
    },
    {
        "category": "streaming_subscriptions",
        "merchants": ["Netflix", "Spotify", "Amazon Prime", "Disney+", "HBO Max",
                      "Apple TV+", "Globoplay", "Crunchyroll", "YouTube Premium", "Deezer"],
        "countries": [("US", 0.70), ("BR", 0.25), ("NL", 0.05)],
        "amount_min": 15,  "amount_max": 120,  "weight": 8,
    },
    {
        "category": "education",
        "merchants": ["Udemy", "Coursera", "Alura", "DIO", "Hotmart", "Eduzz",
                      "Descomplica", "Kroton", "UNIP", "UniCEUB"],
        "countries": [("BR", 0.70), ("US", 0.25), ("IE", 0.05)],
        "amount_min": 30,  "amount_max": 2_000, "weight": 4,
    },
    {
        "category": "clothing_fashion",
        "merchants": ["Zara", "H&M", "Renner", "Riachuelo", "C&A", "Hering",
                      "Farm", "Arezzo", "Vivara", "AMARO"],
        "countries": [("BR", 0.82), ("US", 0.08), ("ES", 0.05), ("SE", 0.05)],
        "amount_min": 60,  "amount_max": 2_500, "weight": 7,
    },
    {
        "category": "electronics_tech",
        "merchants": ["Apple Store", "Samsung", "Fast Shop", "Kabum", "Pichau",
                      "Terabyte", "Dell", "Lenovo Store", "TIM", "Claro"],
        "countries": [("BR", 0.75), ("US", 0.15), ("CN", 0.05), ("KR", 0.05)],
        "amount_min": 100, "amount_max": 15_000,"weight": 5,
    },
    {
        "category": "financial_services",
        "merchants": ["XP Investimentos", "NuInvest", "BTG Digital", "Rico",
                      "Clear Corretora", "Toro Investimentos", "Inter Invest"],
        "countries": [("BR", 0.99), ("US", 0.01)],
        "amount_min": 100, "amount_max": 50_000,"weight": 3,
    },
    {
        "category": "entertainment_leisure",
        "merchants": ["Cinemark", "Ingresso.com", "Ticketmaster", "Steam", "PlayStation Store",
                      "Xbox Game Pass", "Nintendo eShop", "Show da Cidade"],
        "countries": [("BR", 0.65), ("US", 0.30), ("JP", 0.05)],
        "amount_min": 20,  "amount_max": 1_200, "weight": 5,
    },
    {
        "category": "fitness_wellness",
        "merchants": ["SmartFit", "Bodytech", "Academia Bio Ritmo", "Decathlon",
                      "Netshoes Fitness", "Mundo Verde", "Dr. Consulta"],
        "countries": [("BR", 0.97), ("US", 0.02), ("FR", 0.01)],
        "amount_min": 50,  "amount_max": 600,  "weight": 4,
    },
]

# Câmbio simulado por moeda
CURRENCY_RATES = {
    "BRL": 1.00,
    "USD": 5.10,
    "EUR": 5.55,
    "GBP": 6.40,
    "ARS": 0.006,
    "JPY": 0.034,
    "CNY": 0.70,
    "AED": 1.39,
}

COUNTRY_CURRENCY = {
    "BR": "BRL", "US": "USD", "FR": "EUR", "DE": "EUR",
    "ES": "EUR", "IT": "EUR", "PT": "EUR", "NL": "EUR",
    "GB": "GBP", "IE": "EUR", "SE": "EUR",
    "AR": "ARS", "CO": "USD", "JP": "JPY",
    "CN": "CNY", "KR": "USD", "AE": "AED",
}

DECLINE_REASONS = [
    "insufficient_limit", "suspected_fraud", "expired_card",
    "wrong_cvv", "blocked_merchant", "daily_limit_exceeded",
]


# ══════════════════════════════════════════════════════════════
#  FUNÇÕES AUXILIARES
# ══════════════════════════════════════════════════════════════

def weighted_choice(options, weight_key="weight"):
    weights = [o[weight_key] for o in options]
    return random.choices(options, weights=weights, k=1)[0]

def weighted_choice_tuple(options):
    items  = [o[0] for o in options]
    weights = [o[1] for o in options]
    return random.choices(items, weights=weights, k=1)[0]

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def random_datetime(start: date, end: date) -> datetime:
    d = random_date(start, end)
    hour   = random.choices(range(24), weights=[
        1,1,1,1,1,2,3,5,7,8,8,7,7,8,8,8,8,9,9,8,7,6,4,2
    ])[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(d.year, d.month, d.day, hour, minute, second)

def mask_card_number(flag: str) -> str:
    prefixes = {"Visa": "4", "Mastercard": "5", "Elo": "6", "Amex": "3"}
    prefix = prefixes.get(flag, "4")
    last4  = str(random.randint(1000, 9999))
    mid    = "**** ****" if flag != "Amex" else "****** *"
    return f"{prefix}{''.join([str(random.randint(0,9)) for _ in range(3)])} {mid} {last4}"

def generate_cpf() -> str:
    """Gera CPF formatado sem dígitos verificadores reais (apenas simulação)."""
    digits = [random.randint(0, 9) for _ in range(11)]
    return "".join(map(str, digits))

def credit_score_from_income(monthly_income: float) -> int:
    """Score correlacionado com renda e aleatoriedade."""
    base = min(300 + int(monthly_income / 200), 750)
    return max(300, min(850, base + random.randint(-80, 80)))


# ══════════════════════════════════════════════════════════════
#  GERAÇÃO: CUSTOMERS
# ══════════════════════════════════════════════════════════════

print(f"\n[1/3] Gerando {NUM_CUSTOMERS:,} clientes...")

customers = []
cpfs_used = set()

for _ in range(NUM_CUSTOMERS):
    # CPF único
    cpf = generate_cpf()
    while cpf in cpfs_used:
        cpf = generate_cpf()
    cpfs_used.add(cpf)

    gender = random.choices(["M", "F", "NB"], weights=[48, 48, 4])[0]
    if gender == "M":
        name = fake.name_male()
    elif gender == "F":
        name = fake.name_female()
    else:
        name = fake.name()

    birth_date = random_date(date(1950, 1, 1), date(2005, 12, 31))

    region = weighted_choice(BRASILIA_REGIONS)
    zip_code = region["zip_prefix"] + f"-{random.randint(100, 999):03d}"
    street = f"{random.choice(STREET_TYPES)} {random.randint(1, 30)} Bloco {random.choice('ABCDEFGHIJ')} Apt {random.randint(101, 604)}"

    income_band = weighted_choice(INCOME_RANGES)
    monthly_income = round(random.uniform(income_band["min"], income_band["max"]), 2)

    occupation = weighted_choice_tuple(OCCUPATIONS)
    customer_since = random_date(date(2015, 1, 1), date(2024, 12, 31))

    customers.append({
        "cpf":                  cpf,
        "full_name":            name,
        "birth_date":           birth_date.isoformat(),
        "gender":               gender,
        "phone":                fake.phone_number(),
        "email":                fake.email(),
        "address_street":       street,
        "address_neighborhood": region["neighborhood"],
        "address_city":         "Brasília",
        "address_state":        "DF",
        "address_zip":          zip_code,
        "income_range":         income_band["range"],
        "monthly_income":       monthly_income,
        "occupation":           occupation,
        "customer_since":       customer_since.isoformat(),
        "is_active":            random.choices([True, False], weights=[95, 5])[0],
    })

df_customers = pd.DataFrame(customers)
print(f"  ✅ {len(df_customers):,} clientes gerados")


# ══════════════════════════════════════════════════════════════
#  GERAÇÃO: CUSTOMER PROFILES
# ══════════════════════════════════════════════════════════════

print(f"\n[2/3] Gerando {NUM_CUSTOMERS:,} perfis financeiros...")

profiles = []
cpf_to_income = {c["cpf"]: c["monthly_income"] for c in customers}
cpf_to_since  = {c["cpf"]: c["customer_since"]  for c in customers}

for customer in customers:
    cpf = customer["cpf"]
    monthly_income = cpf_to_income[cpf]

    # Card tier correlacionado com renda
    if monthly_income < 3_000:
        tier_weights = [85, 13, 2]
    elif monthly_income < 10_000:
        tier_weights = [45, 45, 10]
    elif monthly_income < 30_000:
        tier_weights = [15, 55, 30]
    else:
        tier_weights = [5, 30, 65]

    card_tier_data = random.choices(CARD_TIERS, weights=tier_weights)[0]
    card_flag      = weighted_choice_tuple(CARD_FLAGS)
    account_type   = weighted_choice_tuple(ACCOUNT_TYPES)

    total_limit     = round(random.uniform(card_tier_data["limit_min"], card_tier_data["limit_max"]), 2)
    available_limit = round(total_limit * random.uniform(0.30, 1.00), 2)
    used_limit      = round(available_limit * random.uniform(0.0, 0.85), 2)
    credit_score    = credit_score_from_income(monthly_income)

    open_date = random_date(
        datetime.strptime(cpf_to_since[cpf], "%Y-%m-%d").date(),
        date(2024, 12, 31)
    )

    account_number = f"{random.randint(10000, 99999)}-{random.randint(0, 9)}"

    profiles.append({
        "cpf":                   cpf,
        "account_type":          account_type,
        "account_number":        account_number,
        "account_open_date":     open_date.isoformat(),
        "card_tier":             card_tier_data["tier"],
        "card_number":           mask_card_number(card_flag),
        "card_flag":             card_flag,
        "total_credit_limit":    total_limit,
        "available_credit_limit":available_limit,
        "used_credit_limit":     used_limit,
        "credit_score":          credit_score,
        "payment_day":           random.randint(1, 28),
        "digital_banking":       random.choices([True, False], weights=[80, 20])[0],
    })

df_profiles = pd.DataFrame(profiles)
print(f"  ✅ {len(df_profiles):,} perfis gerados")


# ══════════════════════════════════════════════════════════════
#  GERAÇÃO: TRANSACTIONS
# ══════════════════════════════════════════════════════════════

print(f"\n[3/3] Gerando transações (jan/2025 – mar/2026)...")

# Perfil de frequência por tier — média de transações por cliente no período
TIER_FREQ = {"gold": 8, "platinum": 14, "black": 22}

cpf_to_tier = {p["cpf"]: p["card_tier"] for p in profiles}
transactions = []

for customer in customers:
    if not customer["is_active"]:
        continue

    cpf  = customer["cpf"]
    tier = cpf_to_tier[cpf]
    # Poisson para número realista de transações por cliente
    n_tx = max(1, int(random.gauss(TIER_FREQ[tier] * 15, TIER_FREQ[tier] * 5)))

    for _ in range(n_tx):
        cat_data   = weighted_choice(MERCHANT_CATEGORIES)
        merchant   = random.choice(cat_data["merchants"])
        country_w  = cat_data["countries"]
        country    = random.choices(
            [c[0] for c in country_w],
            weights=[c[1] for c in country_w]
        )[0]
        currency   = COUNTRY_CURRENCY.get(country, "USD")
        rate       = CURRENCY_RATES.get(currency, 1.0)

        amount_local = round(random.uniform(cat_data["amount_min"], cat_data["amount_max"]), 2)
        amount_brl   = round(amount_local * rate, 2)

        # Parcelamento realista: compras acima de R$200 têm chance maior de parcelar
        if amount_brl > 200 and random.random() < 0.45:
            payment_type = "installment"
            max_inst     = min(12, int(amount_brl / 50))
            installments = random.choices(
                [2, 3, 4, 6, 10, 12],
                weights=[20, 25, 25, 15, 10, 5]
            )[0]
            installments = min(installments, max(2, max_inst))
        else:
            payment_type = "in_cash"
            installments = 1

        installment_amount = round(amount_brl / installments, 2)

        # Status: 94% aprovado, 4% recusado, 2% estornado
        status = random.choices(
            ["approved", "declined", "reversed"],
            weights=[94, 4, 2]
        )[0]
        decline_reason = random.choice(DECLINE_REASONS) if status == "declined" else None

        # Fraude: ~0.3% das transações aprovadas
        is_fraud = (status == "approved") and (random.random() < 0.003)

        purchase_dt = random_datetime(START_DATE, END_DATE)

        # Cidade do merchant (simulada por país)
        merchant_city = fake.city() if country == "BR" else fake.city()

        transactions.append({
            "transaction_id":    str(uuid.uuid4()),
            "cpf":               cpf,
            "purchase_date":     purchase_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "merchant_name":     merchant,
            "merchant_category": cat_data["category"],
            "merchant_city":     merchant_city,
            "merchant_country":  country,
            "amount":            amount_local,
            "currency":          currency,
            "amount_brl":        amount_brl,
            "payment_type":      payment_type,
            "installment_count": installments,
            "installment_amount":installment_amount,
            "card_tier":         tier,
            "status":            status,
            "decline_reason":    decline_reason,
            "is_international":  country != "BR",
            "is_fraud_flag":     is_fraud,
        })

df_transactions = pd.DataFrame(transactions)
df_transactions["purchase_date"] = pd.to_datetime(df_transactions["purchase_date"])
print(f"  ✅ {len(df_transactions):,} transações geradas")

# Stats rápidas
print(f"\n  📊 Resumo das transações:")
print(f"     Aprovadas:      {(df_transactions['status']=='approved').sum():>10,}")
print(f"     Recusadas:      {(df_transactions['status']=='declined').sum():>10,}")
print(f"     Estornadas:     {(df_transactions['status']=='reversed').sum():>10,}")
print(f"     Internacionais: {df_transactions['is_international'].sum():>10,}")
print(f"     Fraudes:        {df_transactions['is_fraud_flag'].sum():>10,}")
print(f"     Parceladas:     {(df_transactions['payment_type']=='installment').sum():>10,}")
print(f"     Ticket médio:   R$ {df_transactions['amount_brl'].mean():>9.2f}")


# ══════════════════════════════════════════════════════════════
#  SALVAR CSV LOCALMENTE
# ══════════════════════════════════════════════════════════════

print("\n[4/4] Salvando arquivos CSV...")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")

files = {
    f"customers_{ts}.csv":         df_customers,
    f"customer_profiles_{ts}.csv": df_profiles,
    f"transactions_{ts}.csv":      df_transactions,
}

for filename, df in files.items():
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    size_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"  ✅ {filename} — {len(df):,} registros — {size_mb:.1f} MB")


# ══════════════════════════════════════════════════════════════
#  UPLOAD PARA S3
# ══════════════════════════════════════════════════════════════

print(f"\n[5/5] Fazendo upload para s3://{BUCKET_NAME}/{S3_PREFIX}")

def criar_bucket_se_necessario(s3_client, bucket, region):
    existing = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    if bucket not in existing:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket)
        else:
            s3_client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"  ✅ Bucket '{bucket}' criado")
    else:
        print(f"  ℹ️  Bucket '{bucket}' já existe")

try:
    s3 = boto3.client("s3", region_name=REGION)
    criar_bucket_se_necessario(s3, BUCKET_NAME, REGION)

    for filename in files:
        s3_key = S3_PREFIX + filename
        s3.upload_file(filename, BUCKET_NAME, s3_key)
        print(f"  ✅ Uploaded → s3://{BUCKET_NAME}/{s3_key}")

    print(f"\n🔗 Verifique no console:")
    print(f"   https://s3.console.aws.amazon.com/s3/buckets/{BUCKET_NAME}")

except Exception as e:
    print(f"\n  ⚠️  Upload falhou: {e}")
    print("  Os arquivos CSV foram salvos localmente com sucesso.")

print("\n" + "=" * 60)
print("  Sprint 1 concluído!")
print("=" * 60)
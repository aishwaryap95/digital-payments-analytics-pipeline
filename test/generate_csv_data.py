import pandas as pd
import random
from datetime import datetime, timedelta
import os

# ===================================================
# CONFIGURATION
# ===================================================
folder = os.getenv("PROJECT_FOLDER", "2026 Plan 5months")

file_location = f"E:\\{folder}\\Project\\spark_data"
os.makedirs(file_location, exist_ok=True)

run_date = datetime.now().strftime("%Y%m%d")

# records per daily drop
daily_record_count = 1000

# fresh data every day, same if rerun same day
random.seed(int(run_date))

# ===================================================
# MASTER DATA
# ===================================================
channels = [
    (1, "UPI", "GPay"),
    (2, "UPI", "PhonePe"),
    (3, "UPI", "Paytm"),
    (4, "CARD", "Visa"),
    (5, "CARD", "Mastercard"),
    (6, "WALLET", "AmazonPay"),
    (7, "NET_BANKING", "HDFC"),
    (8, "QR", "BharatPe")
]

cities = ["Pune", "Mumbai", "Bangalore", "Delhi", "Hyderabad", "Chennai"]

states = {
    "Pune": "Maharashtra",
    "Mumbai": "Maharashtra",
    "Bangalore": "Karnataka",
    "Delhi": "Delhi",
    "Hyderabad": "Telangana",
    "Chennai": "Tamil Nadu"
}

merchant_categories = [
    "Grocery",
    "Food",
    "Travel",
    "Electronics",
    "Healthcare",
    "Fuel",
    "Fashion"
]

# ===================================================
# DIMENSION TABLES
# ===================================================

# dim_payment_channel
df_channel = pd.DataFrame(
    channels,
    columns=["channel_id", "channel_type", "provider_name"]
)

df_channel.to_csv(
    os.path.join(file_location, f"dim_payment_channel_{run_date}.csv"),
    index=False
)

# dim_customer
customers = []

for i in range(1, 101):
    city = random.choice(cities)

    customers.append([
        i,
        f"Customer_{i}",
        random.choice(["18-25", "26-35", "36-50", "50+"]),
        city,
        states[city],
        (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 700))).date(),
        random.choice(["VERIFIED", "PENDING"]),
        random.randint(1, 8)
    ])

df_customer = pd.DataFrame(
    customers,
    columns=[
        "customer_id",
        "customer_name",
        "age_group",
        "city",
        "state",
        "signup_date",
        "kyc_status",
        "preferred_channel_id"
    ]
)

df_customer.to_csv(
    os.path.join(file_location, f"dim_customer_{run_date}.csv"),
    index=False
)

# dim_merchant
merchants = []

for i in range(1, 51):
    city = random.choice(cities)

    merchants.append([
        i,
        f"Merchant_{i}",
        random.choice(merchant_categories),
        city,
        states[city],
        (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 1000))).date(),
        random.choice(["LOW", "MEDIUM", "HIGH"]),
        random.choice(["T+1", "T+2", "T+3"])
    ])

df_merchant = pd.DataFrame(
    merchants,
    columns=[
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "city",
        "state",
        "onboard_date",
        "risk_tier",
        "settlement_cycle"
    ]
)

df_merchant.to_csv(
    os.path.join(file_location, f"dim_merchant_{run_date}.csv"),
    index=False
)

# ===================================================
# FACT TRANSACTIONS
# ===================================================

statuses = (
    ["SUCCESS"] * 78 +
    ["FAILED"] * 12 +
    ["PENDING"] * 7 +
    ["REVERSED"] * 3
)

fail_reasons = [
    "BANK_TIMEOUT",
    "UPI_PIN_FAILED",
    "INSUFFICIENT_FUNDS",
    "NETWORK_ERROR"
]

transactions = []
success_txns = []

for i in range(1, daily_record_count + 1):

    txn_id = f"TXN{run_date}{i:04d}"

    cust = random.randint(1, 100)
    merch = random.randint(1, 50)
    ch = random.randint(1, 8)

    amt = round(random.uniform(10, 10000), 2)

    ts = datetime.now() - timedelta(
        minutes=random.randint(0, 1440)
    )

    status = random.choice(statuses)

    fail_reason = None
    if status == "FAILED":
        fail_reason = random.choice(fail_reasons)

    fee = round(amt * 0.015, 2)

    transactions.append([
        txn_id,
        cust,
        merch,
        ch,
        ts,
        amt,
        status,
        fail_reason,
        random.choice(["ANDROID", "IOS", "WEB"]),
        random.choice(["HDFC", "ICICI", "SBI", "AXIS"]),
        random.choice(cities),
        fee
    ])

    if status == "SUCCESS":
        success_txns.append((txn_id, cust, merch, amt, ts))

df_txn = pd.DataFrame(
    transactions,
    columns=[
        "transaction_id",
        "customer_id",
        "merchant_id",
        "channel_id",
        "transaction_timestamp",
        "transaction_amount",
        "transaction_status",
        "failure_reason",
        "device_type",
        "bank_name",
        "city",
        "processing_fee"
    ]
)

df_txn.to_csv(
    os.path.join(file_location, f"fact_transactions_{run_date}.csv"),
    index=False
)

# ===================================================
# FACT REFUNDS
# ===================================================

refund_count = min(100, len(success_txns))
refunds = []

for i, txn in enumerate(random.sample(success_txns, refund_count), start=1):

    txn_id, cust, merch, amt, ts = txn

    refunds.append([
        f"REF{run_date}{i:04d}",
        txn_id,
        cust,
        ts + timedelta(days=random.randint(1, 10)),
        round(amt * random.uniform(0.30, 1.00), 2),
        random.choice([
            "CUSTOMER_CANCELLED",
            "DUPLICATE_PAYMENT",
            "SERVICE_ISSUE"
        ]),
        "COMPLETED"
    ])

df_refund = pd.DataFrame(
    refunds,
    columns=[
        "refund_id",
        "transaction_id",
        "customer_id",
        "refund_timestamp",
        "refund_amount",
        "refund_reason",
        "refund_status"
    ]
)

df_refund.to_csv(
    os.path.join(file_location, f"fact_refunds_{run_date}.csv"),
    index=False
)

# ===================================================
# FACT SETTLEMENTS
# ===================================================

settlement_statuses = (
    ["SETTLED"] * 90 +
    ["PENDING"] * 10
)

settle_count = min(300, len(success_txns))
settlements = []

for i, txn in enumerate(success_txns[:settle_count], start=1):

    txn_id, cust, merch, amt, ts = txn

    fee = round(amt * 0.015, 2)
    net_amt = round(amt - fee, 2)

    settlements.append([
        f"SET{run_date}{i:04d}",
        merch,
        txn_id,
        (ts + timedelta(days=1)).date(),
        amt,
        fee,
        net_amt,
        random.choice(settlement_statuses)
    ])

df_settlement = pd.DataFrame(
    settlements,
    columns=[
        "settlement_id",
        "merchant_id",
        "transaction_id",
        "settlement_date",
        "gross_amount",
        "fee_amount",
        "net_amount",
        "settlement_status"
    ]
)

df_settlement.to_csv(
    os.path.join(file_location, f"fact_settlements_{run_date}.csv"),
    index=False
)

# ===================================================
# FINAL OUTPUT
# ===================================================

print("CSV files created successfully")
print("Location:", file_location)
print("Run Date:", run_date)
print("Transactions Generated:", daily_record_count)
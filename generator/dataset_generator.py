# generator/dataset_generator.py
import json
import random
import datetime
import uuid
import threading
import os
import numpy as np
from faker import Faker

fake = Faker()

# --- Configuration (réduite pour un PC standard) ---
NUM_USERS = 10000
NUM_PRODUCTS = 5000
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = 50000
NUM_SESSIONS = 200000
TIMESPAN_DAYS = 90
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2
OUTPUT_DIR = "data"

np.random.seed(42)
random.seed(42)
Faker.seed(42)
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Initializing dataset generation...")


def generate_session_id():
    return f"sess_{uuid.uuid4().hex[:10]}"


def generate_transaction_id():
    return f"txn_{uuid.uuid4().hex[:12]}"


class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()

    def update_stock(self, product_id, quantity):
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)


def determine_page_type(position, previous_pages):
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    if not previous_pages:
        return "home"
    prev_page = previous_pages[-1]["page_type"]
    if prev_page == "home":
        return random.choices(["category_listing", "search", "product_detail"], weights=[0.5, 0.3, 0.2])[0]
    elif prev_page == "category_listing":
        return random.choices(["product_detail", "category_listing", "search", "home"], weights=[0.7, 0.1, 0.1, 0.1])[0]
    elif prev_page == "search":
        return random.choices(["product_detail", "search", "category_listing", "home"], weights=[0.6, 0.2, 0.1, 0.1])[0]
    elif prev_page == "product_detail":
        return random.choices(["product_detail", "cart", "category_listing", "search", "home"], weights=[0.3, 0.3, 0.2, 0.1, 0.1])[0]
    elif prev_page == "cart":
        return random.choices(["checkout", "product_detail", "category_listing", "home"], weights=[0.6, 0.2, 0.1, 0.1])[0]
    elif prev_page == "checkout":
        return random.choices(["confirmation", "cart", "home"], weights=[0.8, 0.1, 0.1])[0]
    elif prev_page == "confirmation":
        return random.choices(["home", "product_detail", "category_listing"], weights=[0.6, 0.2, 0.2])[0]
    return "home"


def get_page_content(page_type, products_list, categories_list, inventory):
    if page_type == "product_detail":
        attempts = 0
        while attempts < 10:
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                category = next((c for c in categories_list if c["category_id"] == product["category_id"]), None)
                return product, category
            attempts += 1
        product = random.choice(products_list)
        category = next((c for c in categories_list if c["category_id"] == product["category_id"]), None)
        return product, category
    elif page_type == "category_listing":
        return None, random.choice(categories_list)
    return None, None


# --- Categories ---
categories = []
for cat_id in range(NUM_CATEGORIES):
    category = {
        "category_id": f"cat_{cat_id:03d}",
        "name": fake.company(),
        "subcategories": [],
    }
    for sub_id in range(random.randint(3, 5)):
        category["subcategories"].append({
            "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
            "name": fake.bs(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2),
        })
    categories.append(category)
print(f"Generated {len(categories)} categories")

# --- Products ---
products = []
product_creation_start = datetime.datetime.now() - datetime.timedelta(days=TIMESPAN_DAYS * 2)

for prod_id in range(NUM_PRODUCTS):
    category = random.choice(categories)
    subcategory = random.choice(category["subcategories"])  # ajout: subcategory_id
    base_price = round(random.uniform(5, 500), 2)
    price_history = []

    initial_date = fake.date_time_between(
        start_date=product_creation_start,
        end_date=product_creation_start + datetime.timedelta(days=TIMESPAN_DAYS // 3),
    )
    price_history.append({"price": base_price, "date": initial_date.isoformat()})

    for _ in range(random.randint(0, 2)):
        change_date = fake.date_time_between(start_date=initial_date, end_date="now")
        new_price = round(base_price * random.uniform(0.8, 1.2), 2)
        price_history.append({"price": new_price, "date": change_date.isoformat()})
        initial_date = change_date

    price_history.sort(key=lambda x: x["date"])
    current_price = price_history[-1]["price"]

    products.append({
        "product_id": f"prod_{prod_id:05d}",
        "name": fake.catch_phrase().title(),
        "category_id": category["category_id"],
        "subcategory_id": subcategory["subcategory_id"],
        "base_price": current_price,
        "current_stock": random.randint(10, 1000),
        "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
        "price_history": price_history,
        "creation_date": price_history[0]["date"],
    })
print(f"Generated {len(products)} products")

# --- Users ---
users = []
for user_id in range(NUM_USERS):
    reg_date = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS*3}d", end_date=f"-{TIMESPAN_DAYS}d")
    users.append({
        "user_id": f"user_{user_id:06d}",
        "geo_data": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": fake.country_code(),
        },
        "registration_date": reg_date.isoformat(),
        "last_active": fake.date_time_between(start_date=reg_date, end_date="now").isoformat(),
    })
print(f"Generated {len(users)} users")

# --- Sessions & Transactions ---
inventory = InventoryManager(products)
sessions = []
transactions = []
transaction_counter = 0
session_counter = 0
iteration = 0

print("Generating sessions and transactions...")
while (session_counter < NUM_SESSIONS or transaction_counter < NUM_TRANSACTIONS) and iteration < MAX_ITERATIONS:
    iteration += 1

    if session_counter < NUM_SESSIONS:
        user = random.choice(users)
        session_id = generate_session_id()
        session_start = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now")
        session_duration = random.randint(30, 3600)

        page_views = []
        viewed_products = set()
        cart_contents = {}

        time_slots = sorted([0] + [random.randint(1, session_duration - 1) for _ in range(random.randint(3, 15))] + [session_duration])

        for i in range(len(time_slots) - 1):
            view_duration = time_slots[i + 1] - time_slots[i]
            page_type = determine_page_type(i, page_views)
            product, category = get_page_content(page_type, products, categories, inventory)

            if page_type == "product_detail" and product:
                product_id = product["product_id"]
                viewed_products.add(product_id)
                if random.random() < 0.3:
                    if product_id not in cart_contents:
                        cart_contents[product_id] = {"quantity": 0, "price": product["base_price"]}
                    max_possible = min(3, inventory.get_product(product_id)["current_stock"] - cart_contents[product_id]["quantity"])
                    if max_possible > 0:
                        cart_contents[product_id]["quantity"] += random.randint(1, max_possible)

            page_views.append({
                "timestamp": (session_start + datetime.timedelta(seconds=time_slots[i])).isoformat(),
                "page_type": page_type,
                "product_id": product["product_id"] if product else None,
                "category_id": category["category_id"] if category else None,
                "view_duration": view_duration,
            })

        converted = False
        if cart_contents and any(p["page_type"] in ["checkout", "confirmation"] for p in page_views):
            converted = random.random() < 0.7

        session_geo = user["geo_data"].copy()
        session_geo["ip_address"] = fake.ipv4()

        sessions.append({
            "session_id": session_id,
            "user_id": user["user_id"],
            "start_time": session_start.isoformat(),
            "end_time": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
            "duration_seconds": session_duration,
            "geo_data": session_geo,
            "device_profile": {
                "type": random.choice(["mobile", "desktop", "tablet"]),
                "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            },
            "viewed_products": list(viewed_products),
            "page_views": page_views,
            "cart_contents": {k: v for k, v in cart_contents.items() if v["quantity"] > 0},
            "conversion_status": "converted" if converted else "abandoned" if cart_contents else "browsed",
            "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"]),
        })
        session_counter += 1

        if converted and transaction_counter < NUM_TRANSACTIONS:
            transaction_items = []
            valid = True
            for prod_id, details in cart_contents.items():
                quantity = details["quantity"]
                if quantity > 0:
                    if inventory.update_stock(prod_id, quantity):
                        transaction_items.append({
                            "product_id": prod_id,
                            "quantity": quantity,
                            "unit_price": details["price"],
                            "subtotal": round(quantity * details["price"], 2),
                        })
                    else:
                        valid = False
                        break

            if valid and transaction_items:
                subtotal = round(sum(i["subtotal"] for i in transaction_items), 2)
                discount = 0
                if random.random() < 0.2:
                    discount = round(subtotal * random.choice([0.05, 0.1, 0.15, 0.2]), 2)
                total = round(subtotal - discount, 2)
                transactions.append({
                    "transaction_id": generate_transaction_id(),
                    "session_id": session_id,
                    "user_id": user["user_id"],
                    "timestamp": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
                    "items": transaction_items,
                    "subtotal": subtotal,
                    "discount": discount,
                    "total": total,
                    "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "crypto"]),
                    "status": "completed",
                })
                transaction_counter += 1

    # Transactions supplémentaires sans session
    if transaction_counter < NUM_TRANSACTIONS and random.random() < 0.2:
        user = random.choice(users)
        products_in_txn = random.sample(products, k=min(3, len(products)))
        transaction_items = []
        for product in products_in_txn:
            if product["is_active"]:
                quantity = random.randint(1, 3)
                if inventory.update_stock(product["product_id"], quantity):
                    transaction_items.append({
                        "product_id": product["product_id"],
                        "quantity": quantity,
                        "unit_price": product["base_price"],
                        "subtotal": round(quantity * product["base_price"], 2),
                    })
        if transaction_items:
            subtotal = round(sum(i["subtotal"] for i in transaction_items), 2)
            discount = 0
            if random.random() < 0.2:
                discount = round(subtotal * random.choice([0.05, 0.1, 0.15, 0.2]), 2)
            total = round(subtotal - discount, 2)
            transactions.append({
                "transaction_id": generate_transaction_id(),
                "session_id": None,
                "user_id": user["user_id"],
                "timestamp": fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now").isoformat(),
                "items": transaction_items,
                "subtotal": subtotal,
                "discount": discount,
                "total": total,
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "gift_card"]),
                "status": random.choice(["completed", "processing", "shipped", "delivered"]),
            })
            transaction_counter += 1

    if iteration % 10000 == 0:
        print(f"Progress: {session_counter:,}/{NUM_SESSIONS:,} sessions, {transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions")

# --- Export ---
print("Saving datasets...")


def save(name, obj):
    with open(os.path.join(OUTPUT_DIR, name), "w") as f:
        json.dump(obj, f)


save("users.json", users)
save("products.json", list(inventory.products.values()))
save("categories.json", categories)
save("transactions.json", transactions)

CHUNK_SIZE = 100000
for i in range(0, len(sessions), CHUNK_SIZE):
    save(f"sessions_{i // CHUNK_SIZE}.json", sessions[i:i + CHUNK_SIZE])

print(f"""
Dataset generation complete!
- Sessions: {len(sessions):,}
- Transactions: {len(transactions):,}
- Remaining stock: {sum(p['current_stock'] for p in inventory.products.values()):,}
""")
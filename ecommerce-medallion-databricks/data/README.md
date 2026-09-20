# Source data

The pipeline reads six CSV files from a Unity Catalog Volume:

```
/Volumes/e_commerce/source/dataload/
```

The CSVs themselves are **not committed** to this repository — datasets do not belong in Git. Upload them to the Volume yourself before running `04_transform_source_to_bronze`.

Every file must have a **header row**.

---

## 1. `users.csv`

| Column | Type | Notes |
|---|---|---|
| `user_id` | string | Unique |
| `name` | string | Customer name |
| `email` | string | Email address |
| `gender` | string | |
| `city` | string | |
| `signup_date` | date | |

## 2. `products.csv`

| Column | Type | Notes |
|---|---|---|
| `product_id` | string | Unique |
| `product_name` | string | |
| `category` | string | |
| `brand` | string | |
| `price` | decimal(10,2) | |
| `rating` | decimal(3,2) | |

## 3. `orders.csv`

| Column | Type | Notes |
|---|---|---|
| `order_id` | string | Unique |
| `user_id` | string | FK → users |
| `order_date` | timestamp | |
| `order_status` | string | |
| `total_amount` | decimal(12,2) | |

## 4. `order_items.csv`

| Column | Type | Notes |
|---|---|---|
| `order_item_id` | string | Unique |
| `order_id` | string | FK → orders |
| `product_id` | string | FK → products |
| `user_id` | string | FK → users |
| `quantity` | int | |
| `item_price` | decimal(10,2) | Price per unit |
| `item_total` | decimal(12,2) | Line total |

## 5. `reviews.csv`

| Column | Type | Notes |
|---|---|---|
| `review_id` | string | Unique |
| `order_id` | string | FK → orders |
| `product_id` | string | FK → products |
| `user_id` | string | FK → users |
| `rating` | int | 1 to 5 |
| `review_text` | string | |
| `review_date` | timestamp | |

## 6. `events.csv`

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | Unique |
| `user_id` | string | FK → users |
| `product_id` | string | FK → products |
| `event_type` | string | view, cart, wishlist, purchase |
| `event_timestamp` | timestamp | |

---

## A note on messy data

The source data is intentionally imperfect — inconsistent casing, extra whitespace, duplicates, a few nulls. That is the point. The **silver** layer is where you learn to fix it.

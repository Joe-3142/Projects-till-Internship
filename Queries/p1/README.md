# E-Commerce Revenue & Discount Analysis

**Project 1: My First Project**

## Story / Question

How is revenue distributed across categories and payment methods, and are discounts actually working?

## Dataset

Table: `ecom_data` (staged from `staging_ecom_data`)

| Column | Type | Notes |
|---|---|---|
| `user_id` | text | |
| `product_id` | text | |
| `category` | text | 7 categories: Books, Beauty, Electronics, Sports, Home & Kitchen, Clothing, Toys |
| `price` | numeric | Pre-discount list price |
| `discount_in_percent` | integer | 0, 5, 10, 15, 20, 25, 30, 50 observed |
| `final_price` | numeric | `price * (1 - discount_in_percent / 100)` |
| `payment_method` | text | Credit Card, Debit Card, UPI, Net Banking, Cash on Delivery |
| `purchase_date` | date | See data cleaning note below |

**Grain note:** there is no `order_id` or `quantity` column. Each row is a single item purchase record, not a multi-item shopping cart. Metrics described as "AOV" in this project are therefore average *item* final price, not true average order value — this affects how basket-size claims are worded throughout.

## Data Cleaning

### `purchase_date` format inconsistency

The `purchase_date` column in `staging_ecom_data` contained mixed date formats — some rows in `YYYY-MM-DD` and others in `DD-MM-YYYY`. Loading this directly as a `date` type risks silent misparsing (e.g. `03-04-2024` being read as March 4th in one format and April 3rd in the other).

Fix applied: change the column type to `text` during staging, then normalize to a single consistent date format at insertion time into `ecom_data`.

```sql-bigquery
-- This is needed to fix the initial issue with the purchase_date column in the staging_ecom_data table.
-- The column is currently of type date, but it contains values in different formats
-- (YYYY-MM-DD and DD-MM-YYYY). To handle this, we will change the column type to text
-- and then convert the values to a consistent date format during the insertion into the
-- ecom_data table.
```

## Analysis Process

### 1. Revenue distribution by category, payment method, and discount

Grouped `ecom_data` by `category`, `payment_method`, `discount_in_percent`, computing `category_revenue` and `contribution_percentage` via a nested window function (`SUM(SUM(final_price)) OVER ()`) for the grand total.

A `discount_ranges` CASE statement was added to bucket discounts into No / Low / Medium / High.

**Bug found and fixed:** the initial CASE statement had a gap —

```sql
-- before (buggy): only exact 25% landed in "Medium discount";
-- everything from 11–24% and 26%+ fell into "High discount"
when discount_in_percent <= 10 then 'Low discount'
when discount_in_percent = 25 then 'Medium discount'
else 'High discount'

-- after (fixed): contiguous, non-overlapping bins
when discount_in_percent = 0 then 'No discount'
when discount_in_percent <= 10 then 'Low discount'
when discount_in_percent <= 25 then 'Medium discount'
else 'High discount'
```

### 2. Average item price: discounted vs. non-discounted

Compared average `final_price` for discounted vs. non-discounted items per category, plus a `lift_percentage`. Result: negative lift in every one of the 7 categories, ranging from -15.42% (Books) to -27.99% (Toys).

**Reframing:** this looked like a behavioral finding (customers spend less when discounted) but turned out to be arithmetic — see step 4.

### 3. Bucket-level revenue and item count

Re-aggregated cleanly by the fixed `discount_ranges` bucket (not by raw category+payment_method+discount%, which was too granular to read a trend from) with `bucket_revenue`, `contribution_percentage`, and `order_count` (item count) per bucket:

| Bucket | Revenue | Contribution | Item count |
|---|---|---|---|
| Medium discount | 273,892.48 | 36.17% | 1,358 |
| Low discount | 224,290.49 | 29.62% | 943 |
| High discount | 136,591.31 | 18.04% | 879 |
| No discount | 122,503.80 | 16.18% | 480 |

### 4. Testing whether discounted items just start out cheaper

Hypothesis: maybe discounted items have a lower list price to begin with, which would explain lower final price without any behavioral effect.

Result: `avg_list_price_before_discount` was flat across all four buckets (~251–258). **Hypothesis rejected.** This confirms the lower final price in higher-discount buckets is explained by the discount formula itself (`final_price = price × (1 − discount%)`), not by customers buying cheaper items or spending less.

### 5. Testing whether one category is driving the Medium-discount lead

Used `SUM(...) OVER (PARTITION BY discount_bucket)` to get each category's share of item count within each of the four buckets. Result: category shares stayed close to the ~14.3% even-split baseline (100% / 7 categories) in every bucket, with no single category dominating. **Confound rejected** — the Medium-discount advantage is broad-based, not carried by one or two categories.

## Key Findings

1. Revenue and item volume both peak at **Medium discount (11–25%)** — 36.17% of revenue and 1,358 items — not at No discount or High discount. This is a diminishing-returns curve, not a straight line: discounting works, but returns fall off past ~25%.
2. Discounting is associated with a lower average final price per item, but this is arithmetic, not behavioral — pre-discount list prices are flat across all discount tiers.
3. The Medium-discount lead holds broadly across all seven categories; it isn't propped up by one category outperforming the rest.

## Limitations / Open Questions

- No `order_id` or `quantity` field — findings describe item-level price, not true basket/order value.
- Causality is unresolved: it's unclear whether Medium discount *drives* higher volume, or whether already fast-moving items are simply more likely to be assigned a Medium discount by whoever sets promotions. The dataset has no promotion-assignment logic or purchase-timing-relative-to-discount-change data to settle this.

# Customer CDC and SCD Type 2 Design

## Objective

FinLake implements Change Data Capture and Slowly Changing Dimension Type 2
for the Customer domain.

The objective is to preserve historical customer attributes while maintaining
a single current version for each customer.

## CDC Batch

| Classification | Records |
|---|---:|
| INSERT | 500 |
| UPDATE | 500 |
| UNCHANGED | 9,500 |
| Total | 10,500 |

## Tracked Attributes

The CDC process compares:

- first_name
- last_name
- email
- phone
- city
- customer_segment
- customer_status

## SCD2 Table

Target:

`workspace.finlake_silver.customer_scd2`

Columns:

- customer_id
- first_name
- last_name
- email
- phone
- city
- customer_segment
- customer_status
- effective_from
- effective_to
- is_current
- scd_created_at

## SCD2 Logic

For an updated customer:

1. Identify the existing current version.
2. Expire the existing version.
3. Set `effective_to` to the incoming change date.
4. Insert a new current version.
5. Set `is_current = true` for the new version.

For a new customer:

1. Insert a new current version.
2. Set `effective_from` to the incoming date.
3. Set `effective_to` to the open-ended date.
4. Set `is_current = true`.

## Final State

```text
Total versions       11,000
Unique customers     10,500
Current records      10,500
Historical records      500
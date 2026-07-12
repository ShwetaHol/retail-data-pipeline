select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp as purchased_at,
    order_delivered_customer_date as delivered_at,
    order_estimated_delivery_date as estimated_delivery
from {{ source('raw', 'orders') }}
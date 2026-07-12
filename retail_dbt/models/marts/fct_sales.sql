select
    oi.order_id,
    oi.product_id,
    o.order_status,
    o.purchased_at,
    c.customer_state,
    p.category,
    oi.price,
    oi.freight_value
from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('stg_products') }} p on oi.product_id = p.product_id
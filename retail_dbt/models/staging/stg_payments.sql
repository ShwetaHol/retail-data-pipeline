select
    order_id,
    payment_type,
    payment_installments,
    payment_value
from {{ source('raw', 'order_payments') }}
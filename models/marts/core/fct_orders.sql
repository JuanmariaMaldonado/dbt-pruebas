with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

orders_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from payments
    group by order_id
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(orders_payments.amount,0) as amount -- sustituye los valores NULL por 0
    from orders
    left join orders_payments on orders.order_id = orders_payments.order_id
) 

select * from final


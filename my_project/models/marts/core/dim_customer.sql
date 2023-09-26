with customer as (
    
    select * from {{ ref('stg_tpch_customer') }} 

),
orders as (
    
    select * from {{ ref('stg_tpch_orders') }} 

),
order_item as (
    
    select * from {{ ref('int_order_items') }}

),
order_item_summary as (

    select 
        order_key,
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(item_tax_amount) as item_tax_amount,
        sum(net_item_sales_amount) as net_item_sales_amount
    from order_item
    group by
        1
),
final as (

    select 

        orders.customer_key,

        customer.customer_name,
        customer.account_balance,
        customer.marketing_segment,
        
        orders.order_key, 
        orders.order_date,
        orders.status_code,
        orders.priority_code,
        orders.ship_priority,
                
        order_item_summary.gross_item_sales_amount,
        order_item_summary.item_discount_amount,
        order_item_summary.item_tax_amount,
        order_item_summary.net_item_sales_amount

    from
        orders
        inner join order_item_summary
            on orders.order_key = order_item_summary.order_key

        inner join customer
            on orders.customer_key = customer.customer_key
)
select 
    *
from
    final

order by
    customer_name DESC, order_date DESC
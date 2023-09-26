with source as (

    select * from {{ source('tpch', 'customer') }}

),

renamed as (

    select

        c_custkey as customer_key,
        c_name as customer_name,
        c_address as customer_address,
        c_nationkey as nationkey,
        c_phone as phone,
        c_acctbal as account_balance,
        c_mktsegment as marketing_segment,
        c_comment as comment

    from source

)

select * from renamed

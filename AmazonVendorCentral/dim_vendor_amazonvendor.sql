
select * {{exclude()}} (row_num) from 
    (
    select
    'Amazon Vendor Central' as platform_name,
    vendorid as vendor_id,
    cast(null as string) email,
    cast(null as string) full_name,
    cast(null as string) phone,
    date(lastupdateddate) as last_updated_date,
    row_number() over(partition by vendorid order by _daton_batch_runtime desc) row_num
    from {{ref('RetailProcurementOrdersStatus')}} 
    ) orders
where row_num = 1
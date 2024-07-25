with raw_data as (
    select
        InvoiceNo,
        StockCode,
        Description,
        Quantity,
        cast(InvoiceDate as timestamp) as InvoiceDate,
        cast(UnitPrice as float64) as UnitPrice,
        cast(CustomerID as string) as CustomerID,
        Country
    FROM
        {{ source('retail', 'raw_invoices') }}
)
select * from raw_data
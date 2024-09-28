{{ config(
    materialized='table'
) }}



SELECT 
-- *
    c.CustomerName,
    p.ProductName,
    s.StoreName,
    r.RegionName,
    e.EmployeeName,
    SUM(f.QuantitySold) AS TotalQuantitySold,
    SUM(f.Revenue) AS TotalRevenue,
    AVG(f.Revenue / f.QuantitySold) AS AveragePrice,
    pm.PaymentMethodName,
    pr.PromotionName
FROM 
    read_parquet("/opt/airflow/data-lake/SalesFact.parquet")f
JOIN read_parquet("/opt/airflow/data-lake/DimCustomer.parquet") c ON f.CustomerKey = c.CustomerKey
JOIN read_parquet("/opt/airflow/data-lake/DimProduct.parquet") p ON f.ProductKey = p.ProductKey
JOIN read_parquet("/opt/airflow/data-lake/DimStore.parquet") s ON f.StoreKey = s.StoreKey
JOIN read_parquet("/opt/airflow/data-lake/DimRegion.parquet")  r ON s.Country = r.Country
JOIN read_parquet("/opt/airflow/data-lake/DimEmployee.parquet")  e ON f.EmployeeKey = e.EmployeeKey
JOIN read_parquet("/opt/airflow/data-lake/DimPaymentMethod.parquet")  pm ON f.PaymentMethodKey = pm.PaymentMethodKey
JOIN read_parquet("/opt/airflow/data-lake/DimPromotion.parquet")  pr ON f.PromotionKey = pr.PromotionKey
GROUP BY 
    c.CustomerName, p.ProductName, s.StoreName, r.RegionName, e.EmployeeName, pm.PaymentMethodName, pr.PromotionName
ORDER BY 
    TotalRevenue DESC
Limit 1000000

-- Auto Generated (Do not modify) 94D89976449D1C06EBAC95C7D083632AB9DCB4983A61DAEC9A68EF67B94CA26E
create view "dbo"."my_second_dbt_model" as -- Use the `ref` function to select from other models

select *
from "RetailWarehouse"."dbo"."my_first_dbt_model"
where id = 1;
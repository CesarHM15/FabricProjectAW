-- Auto Generated (Do not modify) 0CC5F69AA2F19F33C37B9D2FACACE7CFF0C984FAAF9032E3BB9E32D50381D0C1
create view "dbo"."clientes_curados" as 
SELECT
    cliente_id,
    nombre_cliente,
    correo,
    pais
FROM dbo.clientes_curados_wh;;
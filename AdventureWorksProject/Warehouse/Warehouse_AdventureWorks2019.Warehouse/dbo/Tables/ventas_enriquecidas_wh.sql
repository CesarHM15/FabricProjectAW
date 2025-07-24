CREATE TABLE [dbo].[ventas_enriquecidas_wh] (

	[producto_id] varchar(8000) NULL, 
	[cliente_id] varchar(8000) NULL, 
	[venta_id] varchar(8000) NULL, 
	[fecha] varchar(8000) NULL, 
	[cantidad] varchar(8000) NULL, 
	[total] varchar(8000) NULL, 
	[nombre_cliente] varchar(8000) NULL, 
	[correo] varchar(8000) NULL, 
	[pais] varchar(8000) NULL, 
	[nombre_producto] varchar(8000) NULL, 
	[categoría] varchar(8000) NULL, 
	[precio] varchar(8000) NULL, 
	[categoria_mayus] varchar(8000) NULL, 
	[descuento_aplicado] bit NULL
);
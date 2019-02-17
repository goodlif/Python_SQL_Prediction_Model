
DROP TABLE IF EXISTS [dbo].[py_customer_clusters];
GO

CREATE TABLE [dbo].[py_customer_clusters](
 [Customer] [bigint] NULL,
 [OrderRatio] [float] NULL,
 [itemsRatio] [float] NULL,
 [monetaryRatio] [float] NULL,
 [frequency] [float] NULL,
 [cluster] [int] NULL,
 ) ON [PRIMARY]
GO

INSERT INTO py_customer_clusters
EXEC [dbo].[py_generate_customer_return_clusters];

SELECT * FROM py_customer_clusters;
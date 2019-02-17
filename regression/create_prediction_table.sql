
DROP TABLE IF EXISTS [dbo].[py_rental_predictions];
GO

CREATE TABLE [dbo].[py_rental_predictions](
 [RentalCount_Predicted] [int] NULL,
 [RentalCount_Actual] [int] NULL,
 [Month] [int] NULL,
 [Day] [int] NULL,
 [WeekDay] [int] NULL,
 [Snow] [int] NULL,
 [Holiday] [int] NULL,
 [Year] [int] NULL
) ON [PRIMARY]
GO
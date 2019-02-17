
USE TutorialDB;
DROP TABLE IF EXISTS rental_py_models;
GO
CREATE TABLE rental_py_models (
	model_name VARCHAR(30) NOT NULL DEFAULT('default model') PRIMARY KEY,
	model VARBINARY(MAX) NOT NULL
);
GO
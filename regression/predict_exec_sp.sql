TRUNCATE TABLE py_rental_predictions;

INSERT INTO py_rental_predictions
EXEC py_predict_rentalcount 'linear_model';

SELECT * FROM py_rental_predictions;
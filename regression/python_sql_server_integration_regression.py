import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from revoscalepy import RxComputeContext, RxInSqlServer, RxSqlServerData
from revoscalepy import rx_import

conn_str = 'Driver=SQL Server;Server=DESKTOP-NLR1KT5;Database=TutorialDB;Trusted_Connection=True;'

column_info = {"Year" : { "type" : "integer" },
         "Month" : { "type" : "integer" },
         "Day" : { "type" : "integer" },
         "RentalCount" : { "type" : "integer" },
         "WeekDay" : {
             "type" : "factor",
             "levels" : ["1", "2", "3", "4", "5", "6", "7"]
         },
         "Holiday" : {
             "type" : "factor",
             "levels" : ["1", "0"]
         },
         "Snow" : {
             "type" : "factor",
             "levels" : ["1", "0"]
         }
     }

data_source = RxSqlServerData(table="dbo.rental_data",
                               connection_string=conn_str, column_info=column_info)
computeContext = RxInSqlServer(
     connection_string = conn_str,
     num_tasks = 1,
     auto_cleanup = False
)


RxInSqlServer(connection_string=conn_str, num_tasks=1, auto_cleanup=False)

df = pd.DataFrame(rx_import(input_data = data_source))
print("Data frame:", df)
columns = df.columns.tolist()
columns = [c for c in columns if c not in ["Year"]]

target = "RentalCount"
train = df.sample(frac=0.8, random_state=1)
test = df.loc[~df.index.isin(train.index)]

print("Training set shape:", train.shape)
print("Testing set shape:", test.shape)

lin_model = LinearRegression()
lin_model.fit(train[columns], train[target])

lin_predictions = lin_model.predict(test[columns])
print("Predictions:", lin_predictions)

lin_mse = mean_squared_error(lin_predictions, test[target])
print("Computed error:", lin_mse)
# <YOUR_IMPORTS>
import os
import pandas as pd
from dill import load
import json


# Укажем путь к файлам проекта:
# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '.')


def predict():
    # <YOUR_CODE>
    models_path = f'{path}/data/models/'
    for x in os.listdir(models_path):
        if x.startswith('cars_pipe_'):
            model_id = x[10:len(x)-4]
            predictions = pd.DataFrame(columns=['car_id', 'pred'])
            model_filename = f'{path}/data/models/{x}'
            with open(model_filename, 'rb') as file:
                model = load(file)
                tests_path = f'{path}/data/test'
                for x_ in os.listdir(tests_path):
                    tes_file_path = f'{tests_path}/{x_}'
                    with open(tes_file_path, 'r') as file_:
                        data = json.load(file_)
                        df = pd.DataFrame.from_dict([data])
                        y = model.predict(df)
                        predictions.loc[len(predictions)] = [data['id'], y[0]]
            prediction_filename = f'{path}/data/predictions/preds_{model_id}.csv'
            predictions.to_csv(prediction_filename, index=False)


if __name__ == '__main__':
    predict()

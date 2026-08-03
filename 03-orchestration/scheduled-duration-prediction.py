#!/usr/bin/env python
import pickle
from datetime import datetime
from pathlib import Path

import mlflow
import pandas as pd
import xgboost as xgb
from dateutil.relativedelta import relativedelta
from prefect import flow, task
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import root_mean_squared_error

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("nyc-taxi-experiment")

models_folder = Path("models")
models_folder.mkdir(exist_ok=True)


@task(retries=3, retry_delay_seconds=10)
def read_dataframe(year, month):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    df = pd.read_parquet(url)

    df["duration"] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ["PULocationID", "DOLocationID"]
    df[categorical] = df[categorical].astype(str)

    df["PU_DO"] = df["PULocationID"] + "_" + df["DOLocationID"]

    return df


@task
def create_X(df, dv=None):
    categorical = ["PU_DO"]
    numerical = ["trip_distance"]
    dicts = df[categorical + numerical].to_dict(orient="records")

    if dv is None:
        dv = DictVectorizer(sparse=True)
        X = dv.fit_transform(dicts)
    else:
        X = dv.transform(dicts)

    return X, dv


@task
def train_model(X_train, y_train, X_val, y_val, dv):
    with mlflow.start_run() as run:
        train = xgb.DMatrix(X_train, label=y_train)
        valid = xgb.DMatrix(X_val, label=y_val)

        best_params = {
            "learning_rate": 0.09585355369315604,
            "max_depth": 30,
            "min_child_weight": 1.060597050922164,
            "objective": "reg:linear",
            "reg_alpha": 0.018060244040060163,
            "reg_lambda": 0.011658731377413597,
            "seed": 42,
        }

        mlflow.log_params(best_params)

        booster = xgb.train(
            params=best_params,
            dtrain=train,
            num_boost_round=30,
            evals=[(valid, "validation")],
            early_stopping_rounds=50,
        )

        y_pred = booster.predict(valid)
        rmse = root_mean_squared_error(y_val, y_pred)
        mlflow.log_metric("rmse", rmse)

        with open("models/preprocessor.b", "wb") as f_out:
            pickle.dump(dv, f_out)
        mlflow.log_artifact("models/preprocessor.b", artifact_path="preprocessor")

        mlflow.xgboost.log_model(booster, artifact_path="models_mlflow")

        return run.info.run_id


@flow
def run(year, month):
    train_date = (
        datetime(year, month, 1) - relativedelta(years=1) - relativedelta(months=1)
    )
    df_train = read_dataframe(year=train_date.year, month=train_date.month)

    val_date = datetime(year, month, 1) - relativedelta(years=1)
    df_val = read_dataframe(year=val_date.year, month=val_date.month)

    X_train, dv = create_X(df_train)
    X_val, _ = create_X(df_val, dv)

    target = "duration"
    y_train = df_train[target].values
    y_val = df_val[target].values

    run_id = train_model(X_train, y_train, X_val, y_val, dv)
    print(f"MLflow run_id: {run_id}")
    return run_id


@flow(
    name="HW3 - STEP 4 : scheduled-flow",
    flow_run_name="run-execution-date-{execution_date.year:04d}-{execution_date.month:02d}",
)
def scheduled_run(execution_date: datetime | None = None):
    if execution_date is None:
        execution_date = datetime.now()
    run_id = run(year=execution_date.year, month=execution_date.month)
    return run_id


if __name__ == "__main__":
    scheduled_run.serve(
        name="hourly-HW3-scheduled-flow",
        cron="06 * * * *",  # S'exécute toutes les heures
    )

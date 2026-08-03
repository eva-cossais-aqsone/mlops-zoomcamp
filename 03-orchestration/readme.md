mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./artifacts
prefect server start
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
cd 03-orchestration    
python duration-prediction.py --year 2024 --month 12
python scheduled-duration-prediction.py
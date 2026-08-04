```
mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./artifacts
prefect server start
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
cd 03-orchestration    
python duration-prediction.py --year 2024 --month 12
python scheduled-duration-prediction.py
```

# STEP 6 : 

**Lancer :**

```
ssh -i "path/mlops-key.pem" ec2-user@<IP-PUBLIQUE-EC2>
```

_dans le terminal ssh :_
```
cd mlops-zoomcamp/03-orchestration
git pull
prefect cloud login -k <CLE_API_PREFECT>
nohup mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./artifacts --host 0.0.0.0 --port 5000 > mlflow.log 2>&1 &
```

```
docker start mlops-worker
``` 
_si pas la première fois, sinon :_

```
docker build -t mlops-pipeline:v1 .
docker run -d --network host --name mlops-worker mlops-pipeline:v1
```
**Mettre en pause :**
```
docker stop mlops-worker
pkill -f mlflow
exit
```
_Sur EC2, trouver l'instance puis instance state>stop (pas terminer)_

**tout terminer**
```
docker stop mlops-worker
docker rm mlops-worker
pkill -f mlflow
exit
```
_Sur EC2, trouver l'instance puis instance state>terminer_
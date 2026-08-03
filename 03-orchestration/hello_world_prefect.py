# prefect server start
# prefect config set PREFECT_API_URL="[http://127.0.0.1:4200/api](http://127.0.0.1:4200/api)"
# python hello_world_prefect.py

from prefect import flow, task


@task
def say_hello():
    print("Hello World!")


@flow
def hello_flow():
    say_hello()


if __name__ == "__main__":
    hello_flow()
    print("Hello World Flow executed successfully!")

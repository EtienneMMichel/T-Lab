FROM python:3.10
RUN mkdir /app 
COPY ./lab_api /app/lab_api
COPY README.md /app

COPY pyproject.toml /app 
COPY app.py /app
WORKDIR /app
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --only main
# CMD ["poetry", "run", "python", "app.py"]
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "app:app", "--bind", "0.0.0.0:8000"]
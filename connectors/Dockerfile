FROM python:3.10
RUN mkdir /app 
COPY ./connectors /app/connectors
COPY README.md /app

COPY pyproject.toml /app 
COPY main.py /app
WORKDIR /app
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --only main
CMD ["poetry", "run", "python", "main.py"]
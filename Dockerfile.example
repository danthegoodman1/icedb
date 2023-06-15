FROM python:3.11

WORKDIR /app

COPY ./requirements.txt .

RUN pip install git+https://github.com/danthegoodman1/icedb
RUN pip install -r requirements.txt

# Pre-install duckdb extensions
RUN mkdir /app/duckdb_exts
RUN cd /app/duckdb_exts && wget https://extensions.duckdb.org/v0.8.0/linux_amd64/json.duckdb_extension.gz
RUN cd /app/duckdb_exts && wget https://extensions.duckdb.org/v0.8.0/linux_amd64/httpfs.duckdb_extension.gz
RUN cd /app/duckdb_exts && gunzip json.duckdb_extension.gz
RUN cd /app/duckdb_exts && gunzip httpfs.duckdb_extension.gz

COPY ./app.py .

CMD [ "python", "app.py" ]

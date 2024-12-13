FROM apache/airflow:2.10.3

USER root
RUN apt-get update && \
    apt install -y default-jdk && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/opt/homebrew/opt/openjdk@11
# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy environment vars
COPY .env /opt/airflow/.env

# Dockerfile
FROM bitnami/spark:latest

# Switch to root user to install packages
USER root

# Fix for the missing partial directory issue and install python3-pip
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get clean && \
    apt-get update && \
    apt-get install -y python3-pip

# Install any additional libraries using pip
RUN pip3 install pandas pyarrow requests beautifulsoup4 lxml

# Switch back to the default user
USER 1001

# Set working directory and copy files
WORKDIR /opt/spark-app
COPY . .

# Define the command to run Spark job
CMD ["spark-submit", "--master", "local", "app/spark_job.py"] && tail -f /dev/null


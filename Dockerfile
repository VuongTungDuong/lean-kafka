FROM python:3.11 

# Set the working directory
WORKDIR /project    

COPY . /project

# Install dependencies
RUN pip install confluent-kafka

RUN chmod -R 777 /project


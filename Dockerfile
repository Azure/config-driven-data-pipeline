FROM python:3.9.13

# Install pylint
RUN pip install --upgrade pip && \
    pip install pylint

# Install git, process tools
RUN apt-get update && apt-get -y install git procps

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;


ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

WORKDIR /usr/src/app
ENV FLASK_APP=./src/app.py
ENV FLASK_RUN_HOST=0.0.0.0
#Server will reload itself on file changes if in dev mode
ENV FLASK_ENV=development 
# Add path for pytest
ENV PYTHONPATH /workspaces/config-driven-data-pipeline/src

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY ./src ./src
COPY ./web ./web
CMD ["flask", "run"]
# We want to go from the base image of python:3.9
FROM python:3.9

# This is the equivalent of “cd /project01” from our host machine
WORKDIR /users

# Let’s copy everything into /project01
COPY . /users

# Installs the dependencies. Passes in a text file.
RUN pip install -r requirements.txt

# This will run when we run our docker container
ENTRYPOINT ["python", "src/main.py"]
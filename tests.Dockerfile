FROM platformmonitoringapi:latest

COPY requirements requirements
RUN pip install -r requirements/test.txt

COPY tests tests

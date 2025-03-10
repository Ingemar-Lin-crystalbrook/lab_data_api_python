FROM python:3.8

RUN pip install snowflake-snowpark-python flask ngrok adyen pytz

COPY ./src /src

WORKDIR /src

EXPOSE 443
CMD python app.py
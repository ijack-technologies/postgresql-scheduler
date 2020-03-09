FROM python:3.7.4-slim-buster

# Install cron for scheduled jobs
RUN apt-get update && \
    apt-get -y --no-install-recommends install cron nano

COPY requirements.txt /
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

# ENV FLASK_APP wsgi.py
# EXPOSE 5005

# COPY app app 
COPY cron.d cron.d
# Copy my preferred .bashrc to /root/ so that it's automatically "sourced" when the container starts
COPY .bashrc /root/
# The main Docker entrypoint when the container starts
COPY .env entrypoint.sh /

CMD ["/bin/bash", "/entrypoint.sh"]

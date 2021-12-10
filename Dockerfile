FROM python:3.8-slim-buster

# Install cron for scheduled jobs and procps for "ps" tool to check running procs
RUN apt-get update && \
    apt-get -y --no-install-recommends install cron nano procps

COPY requirements.txt /
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

# ENV FLASK_APP wsgi.py
# EXPOSE 5005

# COPY app app 
COPY cron_d cron_d
COPY ad_hoc ad_hoc
# Copy my preferred .bashrc to /root/ so that it's automatically "sourced" when the container starts
COPY .bashrc /root/
# The main Docker entrypoint when the container starts
COPY .env entrypoint.sh /
RUN chmod +x /entrypoint.sh

CMD ["/bin/bash", "/entrypoint.sh"]

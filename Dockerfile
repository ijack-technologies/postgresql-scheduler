FROM python:3.8-slim-buster

# Install cron for scheduled jobs and procps for "ps" tool to check running procs
RUN apt-get update && \
    apt-get -y --no-install-recommends install cron nano procps iputils-ping

COPY requirements.txt /
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

WORKDIR /project

COPY project .env entrypoint.sh ./
RUN chmod +x /project/entrypoint.sh && mkdir -p /project/logs

# Copy my preferred .bashrc to /root/ so that it's automatically "sourced" when the container starts
COPY .bashrc /root/

CMD ["/bin/bash", "/project/entrypoint.sh"]

#!/usr/local/bin/python3
PYTHONIOENCODING=utf8
PATH=/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# Start the run once job.
echo "crawler Docker container has been started"

# Setup a cron schedule
echo "0 07-20/2 * * * cd /crawler && /usr/local/bin/python3 runn.py  >> /crawler/log_file 2>&1
# This extra line makes it a valid cron" > scheduler.txt


crontab scheduler.txt
cron -f
# As Scrapy runs on Python, I choose the official Python 3 Docker image.
FROM python:3
#install google chrome
 
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable
RUN apt-get install vim -y

# install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/
RUN apt-get install python3-pymysql -y

RUN mkdir crawler
# Set the working directory to /crawler/
WORKDIR /crawler/
#copy startscript for cron
COPY entrypoint.sh ./

# Copy the file from the local host to the filesystem of the container at the working directory.
COPY requirements.txt ./
 
# Install Scrapy specified in requirements.txt.
RUN pip3 install --no-cache-dir -r requirements.txt
#install cron
RUN apt-get install  cron -y

# set display port to avoid crash
# ENV DISPLAY=:99

# Copy the project source code from the local host to the filesystem of the container at the working directory.
COPY . .
RUN ["ls"]
# give execute permission
RUN ["chmod", "+x", "/crawler/entrypoint.sh"]
RUN ["chmod", "+x", "/usr/local/bin/chromedriver"]

# Run the crawler when the container launches.
ENTRYPOINT ["sh", "entrypoint.sh"]

FROM python:3

WORKDIR /usr/src/app

COPY . .


RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y locales \
    && sed -i -e 's/# pt_PT.UTF-8 UTF-8/pt_PT.UTF-8 UTF-8/' /etc/locale.gen \
    && dpkg-reconfigure --frontend=noninteractive locales \
    && update-locale LANG=pt_PT.UTF-8
ENV LANG pt_PT.UTF-8 
ENV LC_ALL pt_PT.UTF-8

RUN chmod u+x scripts/execute.sh

ENTRYPOINT ["scripts/execute.sh"]

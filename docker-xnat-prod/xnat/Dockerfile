FROM tomcat:7.0.73-jre7

ADD xnattest.war /usr/local/tomcat/webapps/

RUN echo "127.0.0.1 ncanda-back" >> /etc/hosts
RUN cat /etc/hosts

#VOLUME /fs/ncanda-snapshots/test-data /fs/storage/XNAT/archive

RUN mkdir -p /fs/storage/XNAT/archive && \
    mkdir -p /fs/storage/XNAT/prearchive

#RUN useradd -ms /bin/bash tomcat
#USER tomcat

#EXPOSE 8080:8080

CMD ["catalina.sh", "run"]


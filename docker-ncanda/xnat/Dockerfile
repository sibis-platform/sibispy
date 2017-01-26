FROM tomcat:7.0.73-jre7

ADD xnattest.war /usr/local/tomcat/webapps/

RUN echo "127.0.0.1 ncanda-back" >> /etc/hosts
RUN cat /etc/hosts

CMD ["catalina.sh", "run"]


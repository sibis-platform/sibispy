FROM tomcat:7.0.73-jre7

RUN echo "127.0.0.1 ncanda-back" >> /etc/hosts
RUN cat /etc/hosts

RUN mkdir -p /fs/storage/XNAT/archive && \
    mkdir -p /fs/storage/XNAT/prearchive


RUN groupadd tomcat -g 91 && \
    useradd tomcat -u 91 -g 91

RUN chown -R tomcat:tomcat /usr/local/tomcat && \
    ln -s /usr/local/tomcat /var/lib/tomcat

USER tomcat

ADD xnattest.war /usr/local/tomcat/webapps/

CMD ["catalina.sh", "run"]


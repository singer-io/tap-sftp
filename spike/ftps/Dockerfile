#FROM ubuntu:18.04

#COPY ftpd.pem /etc/ftpd-ssl/ftpd.pem
#COPY key.key /etc/ftpd-ssl/key.key

#RUN apt-get update && apt-get -y install \
#    openssl \
#    ftpd-ssl 


#RUN openssl req -x509 -nodes -newkey rsa:2048 -sha256 -keyout /etc/ssl/private/pure-ftpd.pem -out /etc/ssl/private/pure-ftpd.pem -subj "/C=US/ST=PA/O=SingerIo/OU=IT Department/CN=singer-io.com
#RUN openssl dhparam -out /etc/ssl/private/pure-ftpd-dhparams.pem 2048

#RUN openssl req -x509 -nodes -newkey rsa:2048 -sha256 -keyout /etc/ftpd-ssl/key.key -outform pem -out /etc/ftpd-ssl/ftpd.pem -subj "/C=US/ST=PA/O=SingerIo/OU=IT Department/CN=singer-io.com"

#EXPOSE 21
#EXPOSE 990
#EXPOSE 989

#CMD ['ftpd']



FROM stilliard/pure-ftpd

RUN apt-get update && apt-get -y install: python3 python3-pip python3-venv
RUN (echo ftptest; echo ftptest) | pure-pw useradd ftptest -f /etc/pure-ftpd/passwd/pureftpd.passwd -m -u ftpuser -d /home/ftpusers/ftptest

CMD /run.sh -c 30 -C 10 -l puredb:/etc/pure-ftpd/pureftpd.pdb -E -j -R -P $PUBLICHOST -p 30000:30059

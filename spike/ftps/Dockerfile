FROM stilliard/pure-ftpd

RUN apt-get update && apt-get -y install python3 python3-pip python3-venv git docker

RUN mkdir -p /etc/ssl/private
RUN openssl dhparam -out /etc/ssl/private/pure-ftpd-dhparams.pem 2048
RUN openssl req -x509 -nodes -newkey rsa:2048 -sha256 -keyout /etc/ssl/private/pure-ftpd.pem -out /etc/ssl/private/pure-ftpd.pem -subj "/C=UK/ST=Warwickshire/L=Leamington/O=OrgName/OU=IT Department/CN=example.com"
RUN chmod 600 /etc/ssl/private/*.pem

RUN (echo ftptest; echo ftptest) | pure-pw useradd ftptest -f /etc/pure-ftpd/passwd/pureftpd.passwd -m -u ftpuser -d /home/ftpusers/ftptest

CMD /run.sh -c 30 -C 10 -l puredb:/etc/pure-ftpd/pureftpd.pdb -E -j -R -P $PUBLICHOST -p 30000:30059

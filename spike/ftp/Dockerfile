FROM stilliard/pure-ftpd

RUN (echo ftptest; echo ftptest) | pure-pw useradd ftptest -f /etc/pure-ftpd/passwd/pureftpd.passwd -m -u ftpuser -d /home/ftpusers/ftptest

CMD /run.sh -c 30 -C 10 -l puredb:/etc/pure-ftpd/pureftpd.pdb -E -j -R -P $PUBLICHOST -p 30000:30059

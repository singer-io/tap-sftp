# How to start an sftp container with Docker

Pull the image:
- `docker pull atmoz/sftp`

Start the container
- `docker run -p 2222:22 -d atmoz/sftp foo:pass:::upload`
  - user : foo
  - password : pass

Check the IP of the container
```
docker inspect -f '{{.NetworkSettings.IPAddress}}' CONTAINER_NAME
```

Connect with an sftp client:
```
vagrant@taps-alu1:~$ sftp -P 2222 foo@localhost
The authenticity of host '[172.17.0.1]:2222 ([172.17.0.1]:2222)' can't be established.
ED25519 key fingerprint is SHA256:sgEO9dfABF707lwsS7x5nU//0n7J2TurFI5oIgwwGsU.
Are you sure you want to continue connecting (yes/no)? yes
Warning: Permanently added '[172.17.0.1]:2222' (ED25519) to the list of known hosts.
foo@172.17.0.1's password:
Connected to 172.17.0.1.
sftp>
```

Upload a file:
```
vagrant@taps-alu1:/opt/code/tap-sftp/spike$ sftp -P 2222 foo@172.17.0.1
foo@172.17.0.1's password:
Connected to 172.17.0.1.
sftp> pwd
Remote working directory: /
sftp> ls
upload
sftp> cd upload/  # This is what the run command called the directory to create
sftp> ls
sftp> put test_data.csv
Uploading test_data.csv to /upload/test_data.csv
test_data.csv                                                                                                                                                                                               100%   60KB  60.4KB/s   00:00
sftp> ls
test_data.csv
sftp> ^D
```

# Setting up a server we can authenticate with ssh keys

Generate the key pair:
- `ssh-keygen`
- Specify where you want the key to live

Upload public key
```
(tap-sftp) vagrant@taps-alu1:/opt/code/tap-sftp/spike$ docker cp id_rsa.pub modest_hawking:/home/foo/
(tap-sftp) vagrant@taps-alu1:/opt/code/tap-sftp/spike$ docker exec -it modest_hawking /bin/bash
root@ea780f579928:/# cd /home/foo
root@ea780f579928:/home/foo# mkdir .ssh
root@ea780f579928:/home/foo# mv id_rsa.pub .ssh/authorized_keys
root@ea780f579928:/home/foo# exit
```

Test it with `spike.py`:
```
$ python -i spike.py
>>> ftp = connect_with_key()
>>> ftp.listdir()
```

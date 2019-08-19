# How to start an sftp container with Docker

Pull the image:
- `docker pull atmoz/sftp`

Start the container
- `docker run -p 2222:22 -d atmoz/sftp foo:pass:::upload`
  - user : foo
  - password : pass

Check the IP of the container
```
vagrant@taps-alu1:~$ ifconfig
docker0   Link encap:Ethernet  HWaddr 02:42:d3:c9:ce:31
          inet addr:172.17.0.1  Bcast:172.17.255.255  Mask:255.255.0.0
          inet6 addr: fe80::42:d3ff:fec9:ce31/64 Scope:Link
          UP BROADCAST RUNNING MULTICAST  MTU:1500  Metric:1
          RX packets:0 errors:0 dropped:0 overruns:0 frame:0
          TX packets:8 errors:0 dropped:0 overruns:0 carrier:0
          collisions:0 txqueuelen:0
          RX bytes:0 (0.0 B)  TX bytes:648 (648.0 B)
```

Connect with an sftp client:
```
vagrant@taps-alu1:~$ sftp -P 2222 foo@172.17.0.1
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
vagrant@taps-alu1:/opt/code/tap-ftp/spike$ sftp -P 2222 foo@172.17.0.1
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


# Starting a ftps server with Docker

We wrote a custom dockerfile `./dockerfiles/Dockerfile`

`docker build -t tag_it_something .`



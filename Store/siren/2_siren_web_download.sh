# Download file and unzip it
wget {url} -O /home/ec2-user/siren/siren.zip
sleep 10
cd /home/ec2-user/siren && unzip -o /home/ec2-user/siren/siren.zip
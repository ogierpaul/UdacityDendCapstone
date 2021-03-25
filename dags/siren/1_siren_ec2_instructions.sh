# Clean directory before launching
echo Preparing EC2
sudo rm -rf /home/ec2-user/siren
# Make a new directory
mkdir /home/ec2-user/siren -m 777
cd /home/ec2-user/siren

# Download file and unzip it
echo Download siren from {{ var.value.siren_url }}
wget {{ var.value.siren_url }} -O /home/ec2-user/siren/siren.zip
sleep 10
echo {{ Unzipping ziren}}
unzip -o /home/ec2-user/siren/siren.zip
# Go to home directory and copy
echo Copy siren to s3
aws s3 cp /home/ec2-user/siren/{{ var.value.siren_csvname }} s3://{{ var.value.s3_bucket }} /staging/siren_attributes/ {{ var.value.siren_csvname }}

# Clean up
echo Cleaning EC2
cd /home/ec2-user && rm -rf /home/ec2-user/siren/*
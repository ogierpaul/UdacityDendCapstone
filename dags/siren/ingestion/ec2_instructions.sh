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

# Unzip to directory
echo Unzipping siren
unzip -o /home/ec2-user/siren/siren.zip -d /home/ec2-user/siren/
# Add a safety pause
sleep 30
# Log the files in the directory and their size for audit
echo $(ls -sh /home/ec2-user/siren/)
# Go to home directory and copy
echo Copy siren to s3
aws s3 cp  /home/ec2-user/siren/{{ var.value.siren_csvname }}   s3://{{ var.value.s3_bucket }}/staging/siren_attributes/{{ var.value.siren_csvname }}

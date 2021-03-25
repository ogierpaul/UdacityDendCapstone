# Clean directory before launching
echo Preparing ec2
sudo rm -rf /home/ec2-user/infogreffe
# Make a new directory
mkdir /home/ec2-user/infogreffe -m 777

# Download the data
echo Downloading data
curl "{{ var.value.infogreffe_curl }}" -o /home/ec2-user/infogreffe/{{ var.value.infogreffe_csvname }}
echo Download ok
sleep 9

# Copy to s3
echo Copy to s3
aws s3 cp /home/ec2-user/infogreffe/{{ var.value.infogreffe_csvname }} s3://{{ var.value.s3_bucket }}/staging/infogreffe_attributes/{{ var.value.infogreffe_csvname }}
echo Task Completed

# Clean up
echo Cleaning up
cd /home/ec2-user && rm -rf /home/ec2-user/infogreffe/*
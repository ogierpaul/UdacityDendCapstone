# Only write commands on one line as the file will be split with \n
# Install JQ and clean directory before launching
sudo yum install jq -y
cd /home/ec2-user
# Clean directory before launching
sudo rm -rf /home/ec2-user/decp
# Make a new directory
mkdir /home/ec2-user/decp -m 777
aws s3 cp s3://{bucket}/config/jq_marches.sh /home/ec2-user/decp/jq_marches.sh
aws s3 cp s3://{bucket}/config/jq_titulaires.sh /home/ec2-user/decp/jq_titulaires.sh
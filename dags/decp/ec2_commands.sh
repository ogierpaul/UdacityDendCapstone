# Only write commands on one line as the file will be split with \n

echo Templated Parameters {{ var.value.s3_bucket }} {{ var.value.decp_url }}

# Preparing Ec2
# Install JQ and clean directory before launching
echo "Preparing Ec2"
sudo yum install jq -y
cd /home/ec2-user
# Clean directory before launching
sudo rm -rf /home/ec2-user/decp
# Make a new directory
mkdir /home/ec2-user/decp -m 777
# Copy the jq instructions from S3
aws s3 cp s3://{{ var.value.s3_bucket }}/config/jq_marches.sh /home/ec2-user/decp/jq_marches.sh
aws s3 cp s3://{{ var.value.s3_bucket }}/config/jq_titulaires.sh /home/ec2-user/decp/jq_titulaires.sh

## Downloading the file
# Download file and parse with jq to transform it to JSON Lines format
echo "Starting download of file from Web"
wget {{ var.value.decp_url }} -O /home/ec2-user/decp/decp_raw.json

# Prepare the JSON file
echo "Preparing JSON file"
# Format the Json file to a JSON file with Lines, (jq -cr), removing special chars (sed) in the process
cd /home/ec2-user/decp && cat  /home/ec2-user/decp/decp_raw.json | jq -cr '.marches[]' | sed 's/\\[tn]//g' > /home/ec2-user/decp/decp_temp.json

## Marches
echo "Extracting marches informations"
# Extract Marches informations
cat /home/ec2-user/decp/decp_temp.json | jq --compact-output "`cat /home/ec2-user/decp/jq_marches.sh`" > /home/ec2-user/decp/decp_marches.json
# Copy Marches to S3
aws s3 cp /home/ec2-user/decp/decp_marches.json s3://{{ var.value.s3_bucket }}/staging/decp_marches/decp_marches.json

## Titulaires
echo "Extracting Titulaires informations"
# Extract Titulaires informations
cat /home/ec2-user/decp/decp_temp.json | jq --compact-output "`cat /home/ec2-user/decp/jq_titulaires.sh`" > /home/ec2-user/decp/decp_titulaires.json
# Copy Titulaires to S3
aws s3 cp /home/ec2-user/decp/decp_titulaires.json s3://{{ var.value.s3_bucket }}/staging/decp_titulaires/decp_titulaires.json

## Clean EC2
echo "Cleaning EC2"
sudo rm -rf /home/ec2-user/decp

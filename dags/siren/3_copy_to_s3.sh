# Go to home directory and
cd /home/ec2-user/siren && aws s3 cp /home/ec2-user/siren/{csvname} {output_s3}
# Clean up
cd /home/ec2-user && rm -rf /home/ec2-user/siren/*
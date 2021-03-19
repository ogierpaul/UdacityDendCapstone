cat /home/ec2-user/decp/decp_temp.json | jq --compact-output "`cat /home/ec2-user/decp/jq_titulaires.sh`" > /home/ec2-user/decp/decp_titulaires.json

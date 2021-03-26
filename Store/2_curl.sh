echo Downloading infogreffe data
curl "https://opendata.datainfogreffe.fr/api/records/1.0/download/?dataset=chiffres-cles-2020&format=csv&fields=siren,millesime_1,date_de_cloture_exercice_1,duree_1,tranche_ca_millesime_1,ca_1,resultat_1,effectif_1,millesime_2,date_de_cloture_exercice_2,duree_2,tranche_ca_millesime_2,ca_2,resultat_2,effectif_2" -o /home/ec2-user/infogreffe/chiffres-cles-2020.csv
echo sleeping for 9 seconds
sleep 9
aws s3 cp /home/ec2-user/infogreffe/chiffres-cles-2019.csv s3://paulogiereucentral1/staging/infogreffe_attributes/chiffres-cles-2019.csv
echo "task completed"
# rm -rf /home/ec2-user/infogreffe



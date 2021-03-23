curl "https://opendata.datainfogreffe.fr/api/records/1.0/download/?dataset=chiffres-cles-2019&format=csv&fields=denomination,siren,nic,forme_juridique,code_ape,libelle_ape,adresse,code_postal,ville,date_de_cloture_exercice_1,duree_1,ca_1,resultat_1,effectif_1" -o /home/ec2-user/infogreffe/chiffres-cles-2019.csv
echo "sleeping for 9 seconds"
sleep 9
aws s3 cp /home/ec2-user/infogreffe/chiffres-cles-2019.csv s3://paulogiereucentral1/staging/infogreffe_attributes/chiffres-cles-2019.csv
echo "task completed"
# rm -rf /home/ec2-user/infogreffe
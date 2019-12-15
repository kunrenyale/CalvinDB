echo "--------------------sycing to 0 ---------------------";
rsync -r --progress -e "ssh -i ../624-instance-1.pem" ~/Code/classes/databases/final/CalvinDB/src/ ubuntu@ec2-35-172-77-11.compute-1.amazonaws.com:~/CalvinDB/src --delete
echo "--------------------sycing to 1 ---------------------";
rsync -r --progress -e "ssh -i ../624-instance-1.pem" ~/Code/classes/databases/final/CalvinDB/src/ ubuntu@ec2-3-225-84-68.compute-1.amazonaws.com:~/CalvinDB/src --delete
echo "--------------------sycing to 2 ---------------------";
rsync -r --progress -e "ssh -i ../624-instance-1.pem" ~/Code/classes/databases/final/CalvinDB/src/ ubuntu@ec2-3-231-69-11.compute-1.amazonaws.com:~/CalvinDB/src --delete
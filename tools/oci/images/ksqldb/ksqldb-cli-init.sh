# This is the KSQL-DB init script that will be run when the container starts.
# Taken and modified from Robin Moffatt's blog post:
# https://rmoff.net/2018/12/15/docker-tips-and-tricks-with-kafka-connect-ksqldb-and-kafka/#execute-a-ksql-script-through-ksql-cli


echo -e "\n\n⏳ Waiting for KSQL to be available at ${KSQLDB_SERVER_URL} before launching CLI\n"
while [ $(curl -s -o /dev/null -w %{http_code} ${KSQLDB_SERVER_URL}) -eq 000 ]
do 
  echo -e $(date) "KSQL Server HTTP state: " $(curl -s -o /dev/null -w %{http_code} ${KSQLDB_SERVER_URL}) " (waiting for 200)"
  sleep 5
done
echo -e "\n\n-> Running KSQL commands\n"
/bin/ksql --file /home/appuser/init.ksql -- ${KSQLDB_SERVER_URL}
echo -e "\n\n-> Sleeping…\n"
sleep infinity

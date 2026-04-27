#!/bin/bash
set -euo pipefail

# Set env vars
set -a
source .env
set +a

# Rebuilds docker containers (maybe make this optional?)
./gradlew distDocker

# Push new invoker images to each invoker host
docker save whisk/invoker | ssh sp26-cs525-1819.cs.illinois.edu 'docker load'
docker save whisk/invoker | ssh sp26-cs525-1818.cs.illinois.edu 'docker load'
docker save whisk/invoker | ssh sp26-cs525-1817.cs.illinois.edu 'docker load'

# Restarts from scratch, maybe make these steps optional?
cd ansible
ansible-playbook -i environments/$ENVIRONMENT setup.yml
ansible-playbook -i environments/$ENVIRONMENT couchdb.yml
ansible-playbook -i environments/$ENVIRONMENT initdb.yml
ansible-playbook -i environments/$ENVIRONMENT wipe.yml
ansible-playbook -i environments/$ENVIRONMENT openwhisk.yml -e skip_pull_runtimes=true
cd .

# run metrics service
docker run -d --name user-events \
  -p 9095:9095 \
  -e KAFKA_HOSTS=172.17.0.1:9093 \
  whisk/user-events:latest
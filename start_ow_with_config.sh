#!/bin/bash
set -euo pipefail

CONFIG_FILE="${1:-invokers.conf}"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file not found: $CONFIG_FILE" >&2
  exit 1
fi

# Set env vars
set -a
source .env
set +a

# Read invoker hosts (ignore blank/comment lines)
mapfile -t INVOKER_HOSTS < <(awk 'NF && $1 !~ /^#/' "$CONFIG_FILE")

if [[ ${#INVOKER_HOSTS[@]} -eq 0 ]]; then
  echo "No invoker hosts found in $CONFIG_FILE" >&2
  exit 1
fi

echo "Found ${#INVOKER_HOSTS[@]} invoker(s): ${INVOKER_HOSTS[*]}"

# Rebuild docker containers
./gradlew distDocker

# Save once, load on each host
for host in "${INVOKER_HOSTS[@]}"; do
  echo "Pushing whisk/invoker to $host"
  docker save whisk/invoker | ssh "$host" 'docker load'
done


# If your ansible inventory depends on invoker count, ensure that inventory
# is also generated/updated from the same config file before this step.
cd ansible
ansible-playbook -i environments/$ENVIRONMENT setup.yml
ansible-playbook -i environments/$ENVIRONMENT couchdb.yml
ansible-playbook -i environments/$ENVIRONMENT initdb.yml
ansible-playbook -i environments/$ENVIRONMENT wipe.yml
ansible-playbook -i environments/$ENVIRONMENT openwhisk.yml -e skip_pull_runtimes=true
cd ..

docker run -d --name user-events \
  -p 9095:9095 \
  -e KAFKA_HOSTS=172.17.0.1:9093 \
  whisk/user-events:latest
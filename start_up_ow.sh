#!/bin/bash
set -euo pipefail

# Set env vars
set -a
source .env
set +a

# Rebuilds docker containers (maybe make this optional?)
# ./gradlew distDocker

# Restarts from scratch, maybe make these steps optional?
cd ansible
ansible-playbook -i environments/vm controller.yml
ansible-playbook -i environments/vm invoker.yml
cd ..

# Registers toy command for convenience

# Distributed OpenWhisk Setup

## Architecture

```
┌─────────────────────────────────────────────┐
│  sp26-cs525-1820  (control machine)         │
│  Nginx, Controller, Kafka, ZooKeeper,       │
│  CouchDB, Redis, API Gateway, etcd          │
└──────────────┬──────────────────────────────┘
               │ SSH
       ┌───────┼───────┐
       ▼       ▼       ▼
   ┌───────┐┌───────┐┌───────┐
   │ 1819  ││ 1818  ││ 1817  │
   │inv. 0 ││inv. 1 ││inv. 2 │
   └───────┘└───────┘└───────┘
```

All control-plane components run on `sp26-cs525-1820`. Invokers run on three
separate machines, managed over SSH by Ansible from the control machine.

## Prerequisites

- All four machines are RHEL-based (dnf)
- Go is installed on the control machine (for building the wsk CLI)
- You can SSH from the control machine to each invoker host

## Step 1: Run setup on every machine

Copy `setup_vm.sh` to each machine and run it.

**On each invoker** (`1819`, `1818`, `1817`):

```bash
./setup_vm.sh
```

**On the control machine** (`1820`):

```bash
./setup_vm.sh --control
```

Then **log out and back in** on all machines so the docker group membership
takes effect.

## Step 2: Set up SSH keys (from the control machine)

```bash
ssh-keygen -t ed25519   # accept defaults, no passphrase

ssh-copy-id vmraj2@sp26-cs525-1819.cs.illinois.edu
ssh-copy-id vmraj2@sp26-cs525-1818.cs.illinois.edu
ssh-copy-id vmraj2@sp26-cs525-1817.cs.illinois.edu
```

Verify passwordless access:

```bash
ssh sp26-cs525-1819.cs.illinois.edu hostname
ssh sp26-cs525-1818.cs.illinois.edu hostname
ssh sp26-cs525-1817.cs.illinois.edu hostname
```

## Step 3: Build and distribute Docker images

On the control machine:

```bash
cd OpenWhisk
./gradlew distDocker
```

Push the invoker image to each invoker host:

```bash
docker save openwhisk/invoker | ssh sp26-cs525-1819.cs.illinois.edu 'docker load'
docker save openwhisk/invoker | ssh sp26-cs525-1818.cs.illinois.edu 'docker load'
docker save openwhisk/invoker | ssh sp26-cs525-1817.cs.illinois.edu 'docker load'
```

## Step 4: Configure the environment

Ensure `.env` has:

```
ENVIRONMENT=vm
```

The host inventory is defined in `ansible/environments/vm/hosts.j2.ini`. To
change which machines run which components, edit that file. The current layout:

| Component              | Host |
|------------------------|------|
| Nginx, Controller, Kafka, ZooKeeper, CouchDB, Redis, API GW, etcd | `sp26-cs525-1820` |
| Invoker 0              | `sp26-cs525-1819` |
| Invoker 1              | `sp26-cs525-1818` |
| Invoker 2              | `sp26-cs525-1817` |

The `ansible_python_interpreter` in `ansible/environments/vm/group_vars/all`
must point to a valid Python path on all machines (currently `/usr/bin/python`).
Change to `/usr/bin/python3` if needed.

## Step 5: Deploy OpenWhisk

From the control machine:

```bash
cd OpenWhisk
./start_up_ow.sh
```

This runs the Ansible playbooks (`setup`, `couchdb`, `initdb`, `wipe`,
`openwhisk`) against the `vm` environment. Ansible deploys control-plane
containers locally and invoker containers on the remote hosts over SSH.

## Step 6: Configure the wsk CLI

```bash
wsk property set --apihost sp26-cs525-1820.cs.illinois.edu
wsk property set --auth '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP'
```

Test it:

```bash
wsk -i action create fib_wasm wasm_programs/fib.wasm --kind wasm:wasmtime --main _start
wsk -i action invoke fib_wasm --result --param n 10
```

## Troubleshooting

**Ansible can't connect to invoker hosts**: Verify SSH works without a password
prompt. Check that `ansible_connection=ssh` is set for invoker entries in
`hosts.j2.ini`.

**`/var` filling up**: Docker should write to `/srv/docker`. Verify with
`docker info | grep "Docker Root Dir"`. If it still shows `/var/lib/docker`,
restart Docker: `sudo systemctl stop docker docker.socket && sudo systemctl start docker`.

**`http+docker` URL scheme error**: Install `requests<2.32.0` for the Python
that Ansible uses: `pip3 install 'requests<2.32.0'`.

**Invoker container fails to start on remote host**: Ensure the Docker image
was loaded (`docker images | grep openwhisk/invoker` on the invoker host).

**OpenSSL `invalid digest` error during setup**: The `genssl.sh` script
needs `-sha256` instead of `-sha1`. This is already patched in the repo.

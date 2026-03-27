#!/bin/bash
set -euo pipefail

CONTROL=false
if [[ "${1:-}" == "--control" ]]; then
    CONTROL=true
fi

# ── Docker ──────────────────────────────────────────────────────────────────

if ! command -v docker &>/dev/null || ! docker --version 2>/dev/null | grep -q "Docker"; then
    sudo dnf remove docker docker-client docker-client-latest docker-common \
        docker-latest docker-latest-logrotate docker-logrotate docker-engine \
        podman runc -y 2>/dev/null || true

    sudo dnf -y install dnf-plugins-core
    sudo dnf config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
    sudo dnf install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y
fi

sudo usermod -aG docker "$USER"

sudo mkdir -p /srv/docker
echo '{ "data-root": "/srv/docker" }' | sudo tee /etc/docker/daemon.json > /dev/null

sudo systemctl enable docker
sudo systemctl start docker

# ── Python deps (needed for Ansible to manage this host over SSH) ───────────

sudo dnf install python3-pip -y
pip3 install --user docker 'requests<2.32.0'

# ── Control machine extras ──────────────────────────────────────────────────

if [[ "$CONTROL" == true ]]; then
    pip3 install --user ansible==4.1.0 jinja2==3.0.1

    sudo dnf install nodejs -y

    # Build custom wsk CLI
    if [[ ! -d openwhisk-cli ]]; then
        git clone https://github.com/VishruthR/openwhisk-cli.git
    fi
    pushd openwhisk-cli
    go install github.com/go-bindata/go-bindata/go-bindata@latest
    "$(go env GOPATH)/bin/go-bindata" -pkg wski18n -o wski18n/i18n_resources.go wski18n/resources
    go build -o wsk
    wsk_dir="$(pwd)"
    if ! grep -q "$wsk_dir" ~/.bashrc; then
        echo "export PATH=\"$wsk_dir:\$PATH\"" >> ~/.bashrc
    fi
    export PATH="$wsk_dir:$PATH"
    popd

    # Clone OpenWhisk
    if [[ ! -d OpenWhisk ]]; then
        git clone https://github.com/VishruthR/OpenWhisk.git
        cd OpenWhisk
        git fetch --all
        git switch vraj/support-wasm-vm
    fi
fi

echo ""
echo "Setup complete."
if [[ "$CONTROL" == false ]]; then
    echo "This machine is ready to be managed by Ansible as an invoker."
else
    echo "This is the control machine. To deploy OpenWhisk:"
    echo "  cd OpenWhisk && ./start_up_ow.sh"
    echo "Then configure wsk:"
    echo "  wsk property set --apihost <this-host>"
    echo "  wsk property set --auth '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP'"
fi
echo ""
echo "NOTE: Log out and back in (or reboot) for docker group membership to take effect."

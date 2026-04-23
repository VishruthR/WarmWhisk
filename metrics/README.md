# Metrics

This directory contains the necessary instructinos to collect and visualize metrics from OpenWhisk using Prometheus and Grafana.

Metrics are exported by OpenWhisk then analyzed on a local machine so that we can use Grafana's WebUI.

### Prerequisites

- Metrics must be exported by the controllers and invokers (check by curling `https://<host>:<controller-or-invoker-port>/metrics`, you may need to bypass SSL)
- `user-events` service must be running, this should be true if you used `start_up_ow.sh` (verify thru `docker ps`)

# Setting up services

```
# SSH tunnel metrics to local machine
# This will export controller metrics to port 8081 and user-events metrics to 9095 if you used `start_up_ow.sh`
ssh -N -L 8001:127.0.0.1:10001 -L 8005:127.0.0.1:9095 vmraj2@sp26-cs525-1820.cs.illinois.edu
ssh -N -L 8010:127.0.0.1:12001 -L 8510:127.0.0.1:8005 vmraj2@sp26-cs525-1819.cs.illinois.edu
ssh -N -L 8011:127.0.0.1:12002 -L 8511:127.0.0.1:8005 vmraj2@sp26-cs525-1818.cs.illinois.edu
ssh -N -L 8012:127.0.0.1:12003 -L 8512:127.0.0.1:8005 vmraj2@sp26-cs525-1817.cs.illinois.edu

# Start Grafana
docker run -d -p 3000:3000 \
    --name=grafana \
    -v 525-grafana:/var/lib/grafana \
    -e "GF_INSTALL_PLUGINS=leoswing-comparequeries-datasource" \
    grafana/grafana

# Start Prometheus
docker run \
    -p 9090:9090 \
    -v ./prometheus.yml:/etc/prometheus/prometheus.yml \
    -v 525-prometheus:/prometheus \
    --name=prometheus \
    prom/prometheus \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.retention.time=30d
```

You can copy `dashboard.json` into grafana for a convenient default dashboard.

### Troubleshooting

For Grafana and Prometheus to talk to each other, they need to use `host.docker.internal` as their host (at least on Mac).

Soon, I will try to set up a Grafana dashboard that is saved and comes out of the box with useful metrics. Right now, you will have to construct the graphs and queries that you want to see.

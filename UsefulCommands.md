### Start up OpenWhisk

```
# in project root
./gradlew distDocker # builds docker containers

cd ansible
ansible-playbook -i environments/$ENVIRONMENT couchdb.yml
ansible-playbook -i environments/$ENVIRONMENT initdb.yml
ansible-playbook -i environments/$ENVIRONMENT wipe.yml # wipes DB, BE CAREFUL!!!
ansible-playbook -i environments/$ENVIRONMENT openwhisk.yml

# Test using wsk CLI tool
# For many commands you may have to use the -i flag unless you set up certs
wsk property set --apihost 127.0.0.1
wsk property set --auth '23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP' # Default

# Register an action
wsk -i action create hello programs/hello.js --kind nodejs:default

# Run that action
# -r gets the result since actions are invoked asynchronously
wsk -i action invoke hello -r 
```

### CouchDB
You can navigate CouchDB using cURL commands, the web UI at (`http://127.0.0.1:5984/_utils`), or the CouchDB python client.

```
# List all activations
curl -s http://whisk_admin:some_passw0rd@127.0.0.1:5984/whisk_local_activations/_all_docs

# Fetch a specific activation
curl -s -X POST -H "Content-Type: application/json"   -d '{"selector": {"activationId": "51c1309863d044a981309863d0b4a9df"}}'   http://whisk_admin:some_passw0rd@127.0.0.1:5984/whisk_local_activations/_find | jq .
```

### Using with WASM

```
# Create fib action
# when compiling from Rust, "main" gets renamed to "_start"
wsk -i action create fib_wasm wasm_programs/fib.wasm --kind wasm:wasmtime --main _start

# Invoking that action
wsk -i action invoke fib_wasm --param n 10 -r
```

### Developing

```
# Rebuilding a particular container
./gradlew :core:invoker:distDocker

# Re-deploying a particular component
ansible-playbook -i environments/$ENVIRONMENT invoker.yml

# Compiling a rs file with wasmtime
rustc --target wasm32-wasip1 -O "wasm_programs/fib.rs" -o "wasm_programs/fib.wasm"
```

### Benchmarks

```
# Run the benchmark trace
./target/release/wasm-loadgen --trace ../faasrail-shrinkray/artifacts/wasm-trace-spec.csv --minutes 1 2>&1 | head -30
```

### Metrics

```
# system events, exported both by controller and invokers
curl -sk https://127.0.0.1:10001/metrics

# user events
curl http://127.0.0.1:9095/metrics
```

### Data Dependencies
```
wsk -i action create bench_dp /home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip2/release/bench-dp.wasm --kind wasm:wasmtime --main _start

# No data dependency
wsk -i action invoke bench_dp \
    --param url "https://static.vecteezy.com/system/resources/previews/051/679/529/non_2x/sliced-red-strawberry-fruit-on-transparent-background-free-png.png" \
    --param hash "deadbeef" \
    --param filename "strawberry.png" \
    --param max_iter 100 \
    -r

# Data dependency
wsk -i action invoke bench_dp \
    --param url "https://static.vecteezy.com/system/resources/previews/051/679/529/non_2x/sliced-red-strawberry-fruit-on-transparent-background-free-png.png" \
    --param hash "deadbeef" \
    --param filename "strawberry.png" \
    --param max_iter 100 \
    --param data_dependency strawberry.png \
    -r

# seed images for data dependency benchmark
cat > seed_images.sh << 'EOF'
set -e

BASE_URL="https://picsum.photos"
START_INDEX=61
END_INDEX=90

for ((i=$START_INDEX; i<=$END_INDEX; i++)); do
    echo "Downloading image $i"
    curl -L -s -o "image_$i.jpg" "$BASE_URL/id/$i/300"
done

EOF
```
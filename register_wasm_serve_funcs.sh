wsk -i action create bench_aes /home/vmraj2/hello-wasi-http/target/wasm32-wasip1/release/bench_aes.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_chameleon /home/vmraj2/hello-wasi-http/target/wasm32-wasip1/release/bench_chameleon.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_disk_rand /home/vmraj2/hello-wasi-http/target/wasm32-wasip1/release/bench_disk_rand.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_disk_seq /home/vmraj2/hello-wasi-http/target/wasm32-wasip1/release/bench_disk_seq.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_float /home/vmraj2/hello-wasi-http/target/wasm32-wasip1/release/bench_float.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_gzip /home/vmraj2/hello-wasi-http/target/wasm32-wasip1/release/bench_gzip.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_json /home/vmraj2/hello-wasi-http/target/wasm32-wasip1/release/bench_json.wasm --kind wasm:wasmtime --main _start
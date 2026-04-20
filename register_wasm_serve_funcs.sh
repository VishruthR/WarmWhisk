wsk -i action create bench_aes /home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip2/release/bench-aes.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_chameleon /home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip2/release/bench-chameleon.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_disk_rand /home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip2/release/bench-disk-rand.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_disk_seq /home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip2/release/bench-disk-seq.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_float /home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip2/release/bench-float.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_gzip /home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip2/release/bench-gzip.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_json /home/vmraj2/faasrail/faasrail-benchmarks/target/wasm32-wasip2/release/bench-json.wasm --kind wasm:wasmtime --main _start
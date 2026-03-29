wsk -i action create bench_aes ../faasrail/faasrail-benchmarks/target/wasm32-wasip1/release/bench-aes.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_chameleon ../faasrail/faasrail-benchmarks/target/wasm32-wasip1/release/bench-chameleon.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_disk_rand ../faasrail/faasrail-benchmarks/target/wasm32-wasip1/release/bench-disk-rand.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_disk_seq ../faasrail/faasrail-benchmarks/target/wasm32-wasip1/release/bench-disk-seq.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_float ../faasrail/faasrail-benchmarks/target/wasm32-wasip1/release/bench-float.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_gzip ../faasrail/faasrail-benchmarks/target/wasm32-wasip1/release/bench-gzip.wasm --kind wasm:wasmtime --main _start
wsk -i action create bench_json ../faasrail/faasrail-benchmarks/target/wasm32-wasip1/release/bench-json.wasm --kind wasm:wasmtime --main _start
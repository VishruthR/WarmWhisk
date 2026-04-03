wsk -i action create bench_aes ../faasrail/faasrail-benchmarks/crates/bench-aes/src/main.rs --kind rust:1.34
wsk -i action create bench_chameleon ../faasrail/faasrail-benchmarks/crates/bench-chameleon/src/main.rs --kind rust:1.34
wsk -i action create bench_disk_rand ../faasrail/faasrail-benchmarks/crates/bench-disk-rand/src/main.rs --kind rust:1.34
wsk -i action create bench_disk_seq ../faasrail/faasrail-benchmarks/crates/bench-disk-seq/src/main.rs --kind rust:1.34
wsk -i action create bench_float ../faasrail/faasrail-benchmarks/crates/bench-float/src/main.rs --kind rust:1.34
wsk -i action create bench_gzip ../faasrail/faasrail-benchmarks/crates/bench-gzip/src/main.rs --kind rust:1.34
wsk -i action create bench_json ../faasrail/faasrail-benchmarks/crates/bench-json/src/main.rs --kind rust:1.34
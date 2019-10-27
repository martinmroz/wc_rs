# wc_rs
A toy Rust implementation of the C wc utility.

This is a less-than-traditional implementation of the utility built on top of a Flux 
monoid and parallelized with Rayon. The result on my machine was a 2X performance
speed-up and a 10% reduction in memory usage.

Check out the write-up here!

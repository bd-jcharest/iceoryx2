// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::time::Duration;

use clap::Parser;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::update_connections::UpdateConnections;
use iceoryx2::prelude::*;
use iceoryx2::service::port_factory::publish_subscribe::PortFactory;
use iceoryx2_bb_posix::clock::Time;
use iceoryx2_bb_posix::thread::ThreadBuilder;

const ITERATIONS: usize = 1000;

struct Latencies {
    disconnect: Vec<Duration>,
    steady: Vec<Duration>,
}

fn print_percentiles(name: &str, values: &mut [Duration]) {
    values.sort_unstable();
    let percentile = |p: f64| values[((values.len() - 1) as f64 * p) as usize].as_nanos();
    println!(
        "    {name}: p50: {} ns, p99: {} ns, max: {} ns",
        percentile(0.5),
        percentile(0.99),
        percentile(1.0)
    );
}

fn timed_send<T: Service>(
    publisher: &Publisher<T, [u8], ()>,
    payload_size: usize,
) -> Result<Duration, Box<dyn core::error::Error>> {
    let sample = publisher.loan_slice(payload_size)?;
    let start = Time::now()?;
    sample.send()?;
    Ok(start.elapsed()?)
}

fn measure<T: Service>(
    args: &Args,
    service: &PortFactory<T, [u8], ()>,
    allocation_strategy: AllocationStrategy,
) -> Result<Latencies, Box<dyn core::error::Error>> {
    let publisher = service
        .publisher_builder()
        .initial_max_slice_len(args.payload_size)
        .allocation_strategy(allocation_strategy)
        .create()?;
    let persistent_subscriber = service.subscriber_builder().create()?;
    publisher.update_connections()?;

    let mut latencies = Latencies {
        disconnect: Vec::with_capacity(args.iterations),
        steady: Vec::with_capacity(args.iterations),
    };

    for _ in 0..args.iterations {
        let subscriber = service.subscriber_builder().create()?;
        publisher.update_connections()?;
        publisher.loan_slice(args.payload_size)?.send()?;
        drop(subscriber);

        latencies
            .disconnect
            .push(timed_send(&publisher, args.payload_size)?);
        latencies
            .steady
            .push(timed_send(&publisher, args.payload_size)?);

        while persistent_subscriber.receive()?.is_some() {}
    }

    Ok(latencies)
}

fn perform_benchmark<T: Service>(args: &Args) -> Result<(), Box<dyn core::error::Error>> {
    let node = NodeBuilder::new().create::<T>()?;

    for allocation_strategy in [
        AllocationStrategy::Static,
        AllocationStrategy::PowerOfTwo,
        AllocationStrategy::BestFit,
    ] {
        let service = node
            .service_builder(&ServiceName::new("disconnect")?)
            .publish_subscribe::<[u8]>()
            .max_subscribers(args.max_subscribers)
            .subscriber_max_buffer_size(args.subscriber_max_buffer_size)
            .subscriber_max_borrowed_samples(args.subscriber_max_borrowed_samples)
            .history_size(args.history_size)
            .enable_safe_overflow(true)
            .create()?;

        let mut latencies = None;
        ThreadBuilder::new()
            .affinity(&[args.cpu_core])
            .priority(255)
            .spawn(|| {
                latencies =
                    Some(measure(args, &service, allocation_strategy).map_err(|e| e.to_string()))
            })?;

        let mut latencies = latencies.ok_or("the benchmark thread did not report a result")??;

        println!(
            "{} ::: Allocation Strategy: {:?}, Iterations: {}, Max Subscribers: {}, Subscriber Buffer Size: {}, Payload Size: {}",
            core::any::type_name::<T>(),
            allocation_strategy,
            args.iterations,
            args.max_subscribers,
            args.subscriber_max_buffer_size,
            args.payload_size
        );
        print_percentiles(
            "send after a subscriber disconnected",
            &mut latencies.disconnect,
        );
        print_percentiles(
            "send without connection change      ",
            &mut latencies.steady,
        );
    }

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Args {
    /// Number of times a subscriber connects and disconnects
    #[clap(short, long, default_value_t = ITERATIONS)]
    iterations: usize,
    /// Run benchmark for every service setup
    #[clap(short, long)]
    bench_all: bool,
    /// Run benchmark for the IPC zero copy setup
    #[clap(long)]
    bench_ipc: bool,
    /// Run benchmark for the process local setup
    #[clap(long)]
    bench_local: bool,
    /// Activate full log output
    #[clap(short, long)]
    debug_mode: bool,
    /// The cpu core that shall be used by the benchmark
    #[clap(long, default_value_t = 0)]
    cpu_core: usize,
    /// The size in bytes of the payload that shall be used
    #[clap(short, long, default_value_t = 4096)]
    payload_size: usize,
    /// The maximum number of subscribers of the service
    #[clap(long, default_value_t = 16)]
    max_subscribers: usize,
    /// The buffer size of every subscriber
    #[clap(long, default_value_t = 64)]
    subscriber_max_buffer_size: usize,
    /// The maximum number of samples a subscriber can hold at the same time
    #[clap(long, default_value_t = 2)]
    subscriber_max_borrowed_samples: usize,
    /// The history size of the service
    #[clap(long, default_value_t = 1)]
    history_size: usize,
}

fn main() -> Result<(), Box<dyn core::error::Error>> {
    let args = Args::parse();

    if args.debug_mode {
        set_log_level(LogLevel::Trace);
    } else {
        set_log_level(LogLevel::Error);
    }

    let mut at_least_one_benchmark_did_run = false;

    if args.bench_ipc || args.bench_all {
        perform_benchmark::<ipc::Service>(&args)?;
        perform_benchmark::<ipc_threadsafe::Service>(&args)?;
        at_least_one_benchmark_did_run = true;
    }

    if args.bench_local || args.bench_all {
        perform_benchmark::<local::Service>(&args)?;
        perform_benchmark::<local_threadsafe::Service>(&args)?;
        at_least_one_benchmark_did_run = true;
    }

    if !at_least_one_benchmark_did_run {
        println!(
            "Please use either '--bench-all' or select a specific benchmark. See `--help` for details."
        );
    }

    Ok(())
}

#include <iostream>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include "oxigraph/store.h"
#include "oxigraph/model.h"

int main() {
    // Initialize Oxigraph store
    auto store_result = oxigraph::Store::new_default();
    if (!store_result) {
        std::cerr << "Failed to create Oxigraph store." << std::endl;
        return 1;
    }
    oxigraph::Store store = std::move(store_result.value());

    // Prepare benchmark parameters
    const size_t warmup_iterations = 1000;
    const size_t measured_iterations = 10000;
    
    // Sample RDF terms for Quad insertion
    // Subject: http://example.com/subject
    // Predicate: http://example.com/predicate
    // Object: http://example.com/object
    // Graph: http://example.com/graph (Default graph used if empty, or named)
    
    auto s = oxigraph::NamedNode::new_unchecked("http://example.com/subject");
    auto p = oxigraph::NamedNode::new_unchecked("http://example.com/predicate");
    auto o = oxigraph::NamedNode::new_unchecked("http://example.com/object");

    std::vector<long long> latencies_ns;
    latencies_ns.reserve(measured_iterations);

    // Warmup phase
    for (size_t i = 0; i < warmup_iterations; ++i, ++o) {
        // Mutate object slightly to avoid exact duplicate collapsing if not desired, 
        // or just insert the same quad repeatedly to test pure throughput.
        // Let's use a literal that changes per iteration.
        auto literal_obj = oxigraph::Literal::new_simple_literal(std::to_string(i));
        oxigraph::Quad quad(s.clone(), p.clone(), literal_obj, oxigraph::GraphName::DefaultGraph{});
        store.insert(&quad);
    }

    // Reset object for measured runs
    o = oxigraph::NamedNode::new_unchecked("http://example.com/object");

    // Measured phase (Quad Insertion)
    for (size_t i = 0; i < measured_iterations; ++i) {
        auto literal_obj = oxigraph::Literal::new_simple_literal(std::to_string(i + warmup_iterations));
        oxigraph::Quad quad(s.clone(), p.clone(), literal_obj, oxigraph::GraphName::DefaultGraph{});

        auto start = std::chrono::high_resolution_clock::now();
        bool success = store.insert(&quad);
        auto end = std::chrono::high_resolution_clock::now();

        if (!success) {
            std::cerr << "Warning: Quad insertion returned false at iteration " << i << std::endl;
        }

        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        latencies_ns.push_back(duration);
    }

    // Calculations
    std::sort(latencies_ns.begin(), latencies_ns.end());

    long long total_duration_ns = 0;
    for (auto l : latencies_ns) {
        total_duration_ns += l;
    }

    double total_seconds = static_cast<double>(total_duration_ns) / 1e9;
    double ops_sec = static_cast<double>(measured_iterations) / total_seconds;

    long long p50 = latencies_ns[measured_iterations * 50 / 100];
    long long p99 = latencies_ns[measured_iterations * 99 / 100];
    long long p999 = latencies_ns[static_cast<size_t>(measured_iterations * 99.9 / 100)];

    // Format output tabular metrics
    std::cout << "\n===================================================\n";
    std::cout << "      Oxigraph-Core Quad Insertion Microbenchmark    \n";
    std::cout << "===================================================\n";
    std::cout << "| Metric                  | Value                 |\n";
    std::cout << "|-------------------------|-----------------------|\n";
    std::cout << "| Total Iterations        | " << std::setw(21) << measured_iterations << " |\n";
    std::cout << "| Throughput (ops/sec)    | " << std::setw(21) << std::fixed << std::setprecision(2) << ops_sec << " |\n";
    std::cout << "| Latency P50 (ns)        | " << std::setw(21) << p50 << " |\n";
    std::cout << "| Latency P99 (ns)        | " << std::setw(21) << p99 << " |\n";
    std::cout << "| Latency P99.9 (ns)      | " << std::setw(21) << p999 << " |\n";
    std::cout << "===================================================\n\n";

    return 0;
}
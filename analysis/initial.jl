using DataFrames

latency_values = readtable("latencies.csv")

cached_latencies = latency_values[1].data
non_cached_latencies = latency_values[2].data

function print_line()
    println("-------------------------------------------")
end

function print_means()
    cached_latency_mean = mean(latency_values[1].data)
    non_cached_latency_mean = mean(latency_values[2].data)

    println("Cached mean: ", cached_latency_mean)
    println("Non cached latency mean: ", non_cached_latency_mean)
    print("Mean ratio (larger means cached is better): ")
    println("non_cached/cached, ", non_cached_latency_mean/cached_latency_mean)
end

function print_medians()
    cached_latency_median = median(cached_latencies)
    non_cached_latency_median = median(non_cached_latencies)

    println("Median (cached): ", cached_latency_median)
    println("Median (non-cached): ", non_cached_latency_median)
    println("Median (non-cached)/Median (cached): ", non_cached_latency_median/cached_latency_median)
end

function cached_std()
    cached = std(cached_latencies)
    println("Cached standard deviation: ", cached)
end

function noncached_std()
    non_cached_std = std(non_cached_latencies)
    println("Non-cached standard deviation: ", non_cached_std)
end

function box_plot()
    min_cached = minimum(cached_latencies)
    q1_cached = quantile(cached_latencies, 0.25)
    q2_cached = quantile(cached_latencies, 0.5)
    q3_cached = quantile(cached_latencies, 0.75)
    max_cached = maximum(cached_latencies)

    q3_non_cached = quantile(cached_latencies, 1)
    mean_non_cached = mean(cached_latencies)
    sd_non_cached = std(cached_latencies)

    println("Q3 of non_cached: ", q3_non_cached)
    println("One SD above non_cached: ", mean_non_cached + sd_non_cached)
end

print_means()
print_line()

print_medians()
print_line()

cached_std()
noncached_std()
print_line()

box_plot()
print_line()

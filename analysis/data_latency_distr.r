pdf("datacached_latency_distribution.pdf")
ggplot(df, aes(x=cached)) + geom_histogram(colour="chocolate1", fill="chocolate1") + xlab("Panthera (datacached) latency") + ggtitle("Panthera (Datacached) Latency Distribution, nanoseconds")
dev.off()
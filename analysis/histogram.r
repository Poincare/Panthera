dat <- read.csv("latencies.csv")

cached_latency = unlist(na.omit(dat[1]))
no_cached_latency = unlist(na.omit(dat[2]))

pdf("histogram.pdf")
hist(no_cached_latency, breaks=1000, col='black', xlim=c(0, 16000000), ylim=c(0, 600))
hist(cached_latency, breaks=100, col='green', add=T)
dev.off()
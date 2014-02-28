dat <- read.csv("latencies.csv")

cached_latency = unlist(na.omit(dat[1]))
no_cached_latency = unlist(na.omit(dat[2]))

pdf("histogram.pdf")
hist(no_cached_latency, breaks=1000, col='chocolate1', xlim=c(0, 1600000), ylim=c(0, 25))
hist(cached_latency, breaks=10, col='cornflowerblue', add=T)
dev.off()
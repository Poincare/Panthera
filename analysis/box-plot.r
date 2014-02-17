#read in the data
dat <- read.csv("latencies.csv", head = TRUE, sep=",")

cached_data = unlist(na.omit(dat[1]))
noncached_data = unlist(na.omit(dat[2]))

df <- data.frame(cached = cached_data, noncached = noncached_data)
png("boxplot.png")
pdf("boxplot.pdf")
boxplot(df, ylim=c(0, 8000000))
title("Panthera (cached) vs Status Quo (non-cached)")
title(ylab = "Access Time (nanoseconds, lower is better)")
dev.off()
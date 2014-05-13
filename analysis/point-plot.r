dat <- read.csv("latencies.csv")

cached_latency = unlist(na.omit(dat[1]))
no_cached_latency = unlist(na.omit(dat[2]))

pdf("point-plot.pdf")
plot(cached_latency, col="chocolate1", pch=1, cex = 1.5, lwd=2)
points(no_cached_latency, col="cornflowerblue", pch=1, cex = 1.5, lwd=2)
title("Metacached (Panthera) vs Non-metacached\nMultinode")
dev.off()
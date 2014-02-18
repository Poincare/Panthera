dat <- read.csv("latencies.csv")

cached_latency = unlist(na.omit(dat[1]))
no_cached_latency = unlist(na.omit(dat[2]))

pdf("point-plot.pdf")
plot(cached_latency, col="dodgerblue2", pch=1, cex = 1.5, lwd=2)
title("Metacached (Panthera) vs Non-metacached")
points(no_cached_latency, col="chocolate1", pch=1, cex = 1.5, lwd=2)
dev.off()
#read in the data
dat <- read.csv("latencies.csv", head = TRUE, sep=",")

cached_data = unlist(na.omit(dat[1]))
noncached_data = unlist(na.omit(dat[2]))

df <- data.frame(metacached = cached_data, non_metacached = noncached_data)
pdf("boxplot.pdf", width=9.5, height=7)
par(mar=c(5, 4.5, 4, 2))
boxplot(df, ylim=c(0, 4000000), col=c('chocolate1', 'cornflowerblue'), xaxt="n", cex.main=1.5, whisklwd=4, staplelwd=4, outlwd=2, outcex=1.2)
par(cex.main=1.9)
title("Metacached (Panthera) vs Non-metacached (Status Quo)\nMultinode")
par(cex.axis=1.8)
axis(1, at=1:2, labels=c("metacached (Panthera)", "non-metacached"))
par(cex.lab=1.8)
title(ylab = "Access Time (nanoseconds, lower is better)")
dev.off()

#read in the data
dat <- read.csv("latencies.csv", head = TRUE, sep=",")

cached_data = unlist(na.omit(dat[1]))
noncached_data = unlist(na.omit(dat[2]))

df <- data.frame(metacached = cached_data, non_metacached = noncached_data)
pdf("boxplot.pdf")
boxplot(df, ylim=c(0, 4000000), col=c('cornflowerblue', 'chocolate1'), xaxt="n")
title("Metacached (Panthera) vs Non-metacached")
axis(1, at=1:2, labels=c("metacached (panthera)", "non-metacached"))
title(ylab = "Access Time (nanoseconds, lower is better)")
dev.off()

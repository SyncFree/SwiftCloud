data <- read.table("/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/social-responsiveness-config-RR-4/result-social-REPEATABLE_READS-CACHED-true-120000-true-1000.log.ec2-122-248-226-204.ap-southeast-1.compute.amazonaws.com",header = TRUE, sep = ",");

session <- subset(data,data$session_id==1);
write.table(session,"/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/social-timeline-cached.txt",sep = " ");

data2 <- read.table("/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/social-responsiveness-config-RR-1/result-social-REPEATABLE_READS-STRICTLY_MOST_RECENT-false-120000-false-1000.log.ec2-122-248-226-204.ap-southeast-1.compute.amazonaws.com",header = TRUE, sep = ",");

session2 <- subset(data2,data2$session_id==1);
write.table(session2,"/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/social-timeline-notcached.txt",sep = " ");
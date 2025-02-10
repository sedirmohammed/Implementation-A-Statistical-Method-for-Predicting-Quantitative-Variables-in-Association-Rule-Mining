
top_n <- 2
minsup_borders <- c("0.05")

for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    set <- read.csv(paste0("../results/scenario1/churn_for_bank_customers/score__",x,"__", minsup_border, "__set","/part-00000"), header=FALSE)
    colnames(set) <- "score"
    
    set_grouped <- set %>% group_by(score) %>% count()
    set_grouped$rel <- set_grouped$n / sum(set_grouped$n)#
    set_mean <- mean(set$score)
    set_sd <- sd(set$score)
    cdf.upper <- pnorm(set_grouped$score+1, mean = set_mean, sd = set_sd, lower.tail = TRUE)
    cdf.lower <- pnorm(set_grouped$score, mean = set_mean, sd = set_sd, lower.tail = TRUE)
    normal_dist_set_y_prob <- cdf.upper - cdf.lower
    
    
    counter_set <- read.csv(paste0("../results/scenario1/churn_for_bank_customers/score__",x,"__", minsup_border, "__counter_set","/part-00000"), header=FALSE)
    colnames(counter_set) <- "score"
    
    counter_set_grouped <- counter_set %>% group_by(score) %>% count()
    counter_set_grouped$rel <- counter_set_grouped$n / sum(counter_set_grouped$n)
    set_mean <- mean(counter_set$score)
    set_sd <- sd(counter_set$score)
    cdf.upper <- pnorm(counter_set_grouped$score+1, mean = set_mean, sd = set_sd, lower.tail = TRUE)
    cdf.lower <- pnorm(counter_set_grouped$score, mean = set_mean, sd = set_sd, lower.tail = TRUE)
    normal_dist_counter_set_y_prob <- cdf.upper - cdf.lower
    
    distribution <- set_grouped %>% inner_join(counter_set_grouped, by = c("score"))
    distribution <- rename(distribution, rel_counter_set = rel.y)
    distribution <- rename(distribution, rel = rel.x)
    kld_discrete <- round(sum(distribution$rel_counter_set * log(distribution$rel_counter_set/distribution$rel)),4)
    
    static_annotation <- grobTree(textGrob(paste("KLD =", round(kld_discrete, 4)), x=0.15,  y=0.45, hjust=0, gp=gpar(col="black", fontsize=40)))
    pdf(paste0("../results/scenario1/churn_for_bank_customers/churn_for_bank_customers__scenario1__Fig__",x,"__",minsup_border,".pdf"), width = 12, height = 12)
    print(ggplot() + 
      geom_bar(aes(x=set_grouped$score, y= set_grouped$rel), stat="identity",position = "identity", alpha=.4, fill = "steelblue") +
      geom_bar(aes(x=counter_set_grouped$score, y= counter_set_grouped$rel), stat="identity",position = "identity", alpha=.4, fill = "darkorange") +
      labs(x = "credit score", y = "probability") + 
      theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) + 
      annotation_custom(static_annotation) + 
      guides(fill="none"))
    dev.off()
  }
}


minsup_border <- "0.05"
x <- 1
set <- read.csv(paste0("../results/scenario1/churn_for_bank_customers/score__",x,"__", minsup_border, "__set","/part-00000"), header=FALSE)
colnames(set) <- "score"
mean(set$score)
sd(set$score)
IQR(set$score)

minsup_border <- "0.05"
x <- 2
set <- read.csv(paste0("../results/scenario1/churn_for_bank_customers/score__",x,"__", minsup_border, "__set","/part-00000"), header=FALSE)
colnames(set) <- "score"
mean(set$score)
sd(set$score)
IQR(set$score)

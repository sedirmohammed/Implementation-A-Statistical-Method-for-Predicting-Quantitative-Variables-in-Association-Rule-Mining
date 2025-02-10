
top_n <- 1
minsup_borders <- c("0.05")

for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    set <- read.csv(paste0("../results/aumann/churn_for_bank_customers/aumann__score__",x,"__",minsup_border,"__set","/part-00000"), header=FALSE)
    colnames(set) <- "score"
    set <- set %>% select(score)
    
    set_grouped <- set %>% group_by(score) %>% count()
    set_grouped$rel <- set_grouped$n / sum(set_grouped$n)#
    set_mean <- mean(set$score)
    set_sd <- sd(set$score)

    counter_set <- read.csv(paste0("../results/aumann/churn_for_bank_customers/aumann__score__",x,"__",minsup_border,"__counter_set","/part-00000"), header=FALSE)
    colnames(counter_set) <- "score"
    counter_set <- counter_set %>% select(score)
    counter_set_grouped <- counter_set %>% group_by(score) %>% count()
    counter_set_grouped$rel <- counter_set_grouped$n / sum(counter_set_grouped$n)#
    counter_set_mean <- mean(counter_set$score)
    counter_set_sd <- sd(counter_set$score)
    
    
    pdf(paste0("../results/aumann/churn_for_bank_customers/churn_for_bank_customers__aumann__Fig__",x,"__",minsup_border,".pdf"), width = 12, height = 12)
    static_annotation <- grobTree(textGrob(paste("mean difference =", round(set_mean-counter_set_mean, 2)), x=0.08,  y=0.52, hjust=0, gp=gpar(col="black", fontsize=40)))
    print(ggplot() + 
            geom_bar(aes(x=set_grouped$score, y= set_grouped$rel), stat="identity",position = "identity", alpha=.4, fill = "steelblue") +
            geom_bar(aes(x=counter_set_grouped$score, y= counter_set_grouped$rel), stat="identity",position = "identity", alpha=.4, fill = "darkorange") +
            labs(x = "credit score", y = "probability") + 
            theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) + 
            annotation_custom(static_annotation) + 
            guides(fill="none") +
            geom_segment(aes(x = counter_set_mean, y = 0, xend = counter_set_mean, yend = 0.01), color = "darkorange2", linetype="dotted", size=1.5)+
            geom_segment(aes(x = set_mean, y = 0, xend = set_mean, yend = 0.01), color = "steelblue4", linetype="dotted", size=1.5))
    dev.off()
  }
}  

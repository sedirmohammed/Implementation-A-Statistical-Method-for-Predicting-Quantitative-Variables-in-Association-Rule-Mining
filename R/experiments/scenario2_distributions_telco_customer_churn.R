
top_n <- 2
minsup_borders <- c("0.05")

for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    set <- read.csv(paste0("../results/scenario2/telco_customer_churn/tenure__",x,"__", minsup_border, "__set","/part-00000"))
    colnames(set) <- "tenure"
    counter_set <- read.csv(paste0("../results/scenario2/telco_customer_churn/tenure__",x,"__", minsup_border, "__counter_set","/part-00000"))
    colnames(counter_set) <- "tenure"
    
    lambda_set <- 1/sum(set$tenure/nrow(set))
    lambda_counterSet <- 1/sum(counter_set$tenure/nrow(counter_set))
    lp <- lambda_counterSet
    lq <- lambda_set
    kld <- log(lp)-log(lq)+(lq/lp)-1
    
    
    set_grouped <- set %>% group_by(tenure) %>% count()
    set_grouped$rel <- set_grouped$n / sum(set_grouped$n)
    set_grouped$expo_set_px <- exp(-lambda_set*set_grouped$tenure) - exp(-lambda_set*(set_grouped$tenure+1))
    
    counter_set_grouped <- counter_set %>% group_by(tenure) %>% count()
    counter_set_grouped$rel <- counter_set_grouped$n / sum(counter_set_grouped$n)
    counter_set_grouped$expo_set_px <- exp(-lambda_counterSet*counter_set_grouped$tenure) - exp(-lambda_counterSet*(counter_set_grouped$tenure+1))
    
    
    static_annotation <- grobTree(textGrob(paste("KLD =", round(kld, 4)), x=0.35,  y=0.25, hjust=0, gp=gpar(col="black", fontsize=40)))
    pdf(paste0("../results/scenario2/telco_customer_churn/telco_customer_churn__scenario2__Fig__",x,"__",minsup_border,".pdf"), width = 12, height = 12)
    print(ggplot() + 
      geom_bar(aes(x=setGrouped$tenure, y= setGrouped$rel), stat="identity",position = "identity", alpha=.4, fill = "steelblue") +
      geom_bar(aes(x=counter_setGrouped$tenure, y= counter_setGrouped$rel), stat="identity",position = "identity", alpha=.4, fill = "darkorange") +
      geom_line(aes(x=set_grouped$tenure, y=set_grouped$expo_set_px), col = "steelblue4", linewidth=1.4) +
      geom_line(aes(x=counter_set_grouped$tenure, y=counter_set_grouped$expo_set_px), col = "darkorange2", linewidth=1.4) + 
      labs(x = "tenure", y = "probability") + 
      theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) + 
      annotation_custom(static_annotation) + 
      guides(fill="none"))
    dev.off()
  }
}

minsup_border <- "0.05"
x <- 1
set <- read.csv(paste0("../results/scenario2/telco_customer_churn/tenure__",x,"__",minsup_border,"__set","/part-00000"), header=FALSE)
colnames(set) <- "tenure"
mean(set$tenure)
sd(set$tenure)
IQR(set$tenure)

x <- 2
set <- read.csv(paste0("../results/scenario2/telco_customer_churn/tenure__",x,"__",minsup_border,"__set","/part-00000"), header=FALSE)
colnames(set) <- "tenure"
mean(set$tenure)
sd(set$tenure)
IQR(set$tenure)


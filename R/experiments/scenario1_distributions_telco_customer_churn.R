
top_n <- 2
minsup_borders <- c("0.05")

for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    set <- read.csv(paste0("../results/scenario1/telco_customer_churn/tenure__",x,"__",minsup_border,"__set","/part-00000"), header=FALSE)
    colnames(set) <- "tenure"
    set$tenure <- as.integer(set$tenure)   
    set <- set %>% group_by(tenure) %>% count()
    set$rel <- set$n / sum(set$n)
    
    counter_set <- read.csv(paste0("../results/scenario1/telco_customer_churn/tenure__",x,"__",minsup_border,"__counter_set","/part-00000"), header=FALSE)
    colnames(counter_set) <- "tenure"
    counter_set$tenure <- as.integer(counter_set$tenure)    
    counter_set <- counter_set %>% group_by(tenure) %>% count()
    counter_set$rel_counter_set <- counter_set$n / sum(counter_set$n)
    
    distribution <- inner_join(set, counter_set, by="tenure")
    plot_data <- distribution %>% select(tenure, rel, rel_counter_set)
    
    
    kld <- round(sum(distribution$rel_counter_set * log(distribution$rel_counter_set/distribution$rel)),4)
    static_annotation <- grobTree(textGrob(paste("KLD =", kld), x=0.17,  y=0.45, hjust=0,
                                           gp=gpar(col="black", fontsize=40)))
    
    melted <- melt(distribution %>% select(tenure, rel, rel_counter_set), id="tenure")
    
    pdf(paste0("../results/scenario1/telco_customer_churn/telco_customer_churn__scenario1__Fig__",x,"__",minsup_border,".pdf"), width = 12, height = 12)
    print(ggplot(melted,aes(x=tenure,y=value,fill=variable)) + 
            geom_bar(stat="identity", position = "identity", alpha=.4) +
            labs(x = "tenure", y = "probability") +
            scale_fill_manual("legend", values = c("rel" = "steelblue", "rel_counter_set" = "darkorange")) + 
            theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) +
            annotation_custom(static_annotation) + 
            guides(fill="none"))
    dev.off()
  }
}

minsup_border <- "0.05"
x <- 1
set <- read.csv(paste0("../results/scenario1/telco_customer_churn/tenure__",x,"__",minsup_border,"__set","/part-00000"), header=FALSE)
colnames(set) <- "tenure"
mean(set$tenure)
sd(set$tenure)
IQR(set$tenure)

x <- 2
set <- read.csv(paste0("../results/scenario1/telco_customer_churn/tenure__",x,"__",minsup_border,"__set","/part-00000"), header=FALSE)
colnames(set) <- "tenure"
mean(set$tenure)
sd(set$tenure)
IQR(set$tenure)


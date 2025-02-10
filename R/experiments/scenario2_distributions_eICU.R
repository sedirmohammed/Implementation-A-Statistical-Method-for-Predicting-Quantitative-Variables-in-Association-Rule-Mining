
top_n <- 2
minsup_borders <- c("0.025", "0.05", "0.075")
for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    set <- read.csv(paste0("../results/scenario2/eICU/icd10/los__",x,"__",minsup_border, "__set","/part-00000"), header=FALSE)
    colnames(set) <- "LOS"
    counter_set <- read.csv(paste0("../results/scenario2/eICU/icd10/los__",x,"__",minsup_border, "__counter_set","/part-00000"), header=FALSE)
    colnames(counter_set) <- "LOS"
    lambda_set <- 1/sum(set$LOS/nrow(set))
    lambda_counterSet <- 1/sum(counter_set$LOS/nrow(counter_set))
    lp <- lambda_counterSet
    lq <- lambda_set
    kld <- log(lp)-log(lq)+(lq/lp)-1
    
    set_grouped <- set %>% group_by(LOS) %>% count()
    set_grouped$rel <- set_grouped$n / sum(set_grouped$n)
    set_grouped$expo_set_px <- exp(-lambda_set*set_grouped$LOS) - exp(-lambda_set*(set_grouped$LOS+1))
    
    counter_set_grouped <- counter_set %>% group_by(LOS) %>% count()
    counter_set_grouped$rel <- counter_set_grouped$n / sum(counter_set_grouped$n)
    counter_set_grouped$expo_set_px <- exp(-lambda_counterSet*counter_set_grouped$LOS) - exp(-lambda_counterSet*(counter_set_grouped$LOS+1))
    
    static_annotation <- grobTree(textGrob(paste("KLD =", round(kld, 4)), x=0.35,  y=0.25, hjust=0,
                                           gp=gpar(col="black", fontsize=40)))
    
    pdf(paste0("../results/scenario2/eICU/icd10/eICU__scenario2__Fig__",x,"__",minsup_border,".pdf"), width = 12, height = 12)
    print(ggplot() + 
            geom_line(aes(x=set_grouped$LOS, y= set_grouped$expo_set_px), col = "steelblue4", linewidth=1.4) + 
            geom_line(aes(x=counter_set_grouped$LOS, y= counter_set_grouped$expo_set_px), col = "darkorange2", linewidth=1.4)+
            geom_bar(aes(x=set_grouped$LOS, y= set_grouped$rel), stat="identity",position = "identity", alpha=.4, fill = "steelblue") + 
            geom_bar(aes(x=counter_set_grouped$LOS, y= counter_set_grouped$rel), stat="identity",position = "identity", alpha=.4, fill = "darkorange") + 
            labs(x = "ICU LOS", y = "probability") +
            theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) + 
            annotation_custom(static_annotation)
    )
    dev.off()
  }
}






top_n <- 2
minsup_borders <- c("0.025","0.05", "0.075")
fig_counter <- 1
for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    set <- read.csv(paste0("../results/scenario1/mimic/los__",x,"__",minsup_border, "__set","/part-00000"))
    colnames(set) <- "LOS"
    
    set$LOS <- as.integer(set$LOS)   
    set <- set %>% group_by(LOS) %>% count()
    set$rel <- set$n / sum(set$n)
    
    counter_set <- read.csv(paste0("../results/scenario1/mimic/los__",x,"__",minsup_border,"__counter_set","/part-00000"))
    colnames(counter_set) <- "LOS"
    counter_set$LOS <- as.integer(counter_set$LOS)    
    counter_set <- counter_set %>% group_by(LOS) %>% count()
    counter_set$rel_counter_set <- counter_set$n / sum(counter_set$n)
    
    distribution <- inner_join(set, counter_set, by="LOS")
    plot_data <- distribution %>% select(LOS, rel, rel_counter_set)
    
    
    kld <- round(sum(distribution$rel_counter_set * log(distribution$rel_counter_set/distribution$rel)),4)
    static_annotation <- grobTree(textGrob(paste("KLD =", kld), x=0.35,  y=0.25, hjust=0,
                                           gp=gpar(col="black", fontsize=40)))
    
    melted <- melt(distribution %>% select(LOS, rel, rel_counter_set), id="LOS")
    
    pdf(paste0("../results/scenario1/mimic/mimic__scenario1__Fig__",x,"__",minsup_border,".pdf"), width = 12, height = 12)
    print(ggplot(melted,aes(x=LOS,y=value,fill=variable)) + 
      geom_bar(stat="identity",position = "identity", alpha=.4) +
      labs(x = "ICU LOS", y = "probability") +
      scale_fill_manual("legend", values = c("rel" = "steelblue", "rel_counter_set" = "darkorange")) + 
      theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) +
      annotation_custom(static_annotation) + 
      guides(fill="none"))
    dev.off()
    fig_counter <- fig_counter + 1
  }
}


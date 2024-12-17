
top_n <- 2
minsup_borders <- c("0.05")
for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    kld_vec <- c()
    for (resample in 1:10000) {
      
      set <- read.csv(paste0("../results/scenario1/telco_customer_churn/tenure__",x,"__",minsup_border,"__set","/part-00000"), header=FALSE)
      colnames(set) <- "tenure"
      set$tenure <- as.integer(set$tenure)
      set <- set[sample(nrow(set), nrow(set),replace = TRUE), ]
      set <- set %>% as_tibble() %>% rename(tenure = value)
      set <- set %>% group_by(tenure) %>% count()
      set$rel <- set$n / sum(set$n)
      
      counter_set <- read.csv(paste0("../results/scenario1/telco_customer_churn/tenure__",x,"__", minsup_border,"__counter_set","/part-00000"), header=FALSE)
      colnames(counter_set) <- "tenure"
      counter_set$tenure <- as.integer(counter_set$tenure)
      counter_set <- counter_set[sample(nrow(counter_set), nrow(counter_set),replace = TRUE), ]
      counter_set <- counter_set %>% as_tibble() %>% rename(tenure = value)
      counter_set <- counter_set %>% group_by(tenure) %>% count()
      counter_set$rel_counter_set <- counter_set$n / sum(counter_set$n)
      
      distribution <- inner_join(set, counter_set, by="tenure")
      plot_data <- distribution %>% select(tenure, rel, rel_counter_set)
      
      kld <- round(sum(distribution$rel_counter_set * log(distribution$rel_counter_set/distribution$rel)),4)
      kld_vec <- append(kld_vec, kld)
    }
    kld_sd <- round(sd(kld_vec), 2)
    print(paste0("Error bars scenario 1 tenure__",x,"__",minsup_border, ": ", kld_sd))
  }
}

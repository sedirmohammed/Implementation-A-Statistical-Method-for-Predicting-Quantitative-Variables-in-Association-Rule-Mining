
top_n <- 2
minsup_borders <- c("0.05")
for (minsup_border in minsup_borders){
  for (x in 2:top_n) {
    kld_vec <- c()
    for (resample in 1:10000) {
      
      set <- read.csv(paste0("../results/scenario2/churn_for_bank_customers/score__",x,"__",minsup_border,"__set","/part-00000"), header=FALSE)
      colnames(set) <- "score"
      set$score <- as.integer(set$score)
      set <- set[sample(nrow(set), nrow(set),replace = TRUE), ]
      set <- set %>% as_tibble() %>% rename(score = value)
      set <- set %>% group_by(score) %>% count()
      set$rel <- set$n / sum(set$n)
      
      counter_set <- read.csv(paste0("../results/scenario2/churn_for_bank_customers/score__", x,"__", minsup_border,"__counter_set","/part-00000"), header=FALSE)
      colnames(counter_set) <- "score"
      counter_set$score <- as.integer(counter_set$score)
      counter_set <- counter_set[sample(nrow(counter_set), nrow(counter_set),replace = TRUE), ]
      counter_set <- counter_set %>% as_tibble() %>% rename(score = value)
      counter_set <- counter_set %>% group_by(score) %>% count()
      counter_set$rel_counter_set <- counter_set$n / sum(counter_set$n)
      
      distribution <- inner_join(set, counter_set, by="score")
      plot_data <- distribution %>% select(score, rel, rel_counter_set)
      
      kld <- round(sum(distribution$rel_counter_set * log(distribution$rel_counter_set/distribution$rel)),4)
      kld_vec <- append(kld_vec, kld)
    }
    kld_sd <- round(sd(kld_vec), 2)
    print(paste0("Error bars scenario 2 score__",x,"__",minsup_border, ": ", kld_sd))
  }
}

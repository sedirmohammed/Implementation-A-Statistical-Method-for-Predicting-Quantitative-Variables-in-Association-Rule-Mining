
top_n <- 2
minsup_borders <- c("0.025" ,"0.05", "0.075")

for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    kld_vec <- c()
    for (resample in 1:10000) {
      
      set <- read.csv(paste0("../results/scenario1/eICU/icd10/los__",x,"__",minsup_border,"__set","/part-00000"))
      colnames(set) <- "LOS"
      set$LOS <- as.integer(set$LOS)
      set <- set[sample(nrow(set), nrow(set), replace = TRUE), ]
      set <- set %>% as_tibble() %>% rename(LOS = value)
      set <- set %>% group_by(LOS) %>% count()
      set$rel = set$rel <- set$n / sum(set$n)
      
      counter_set <- read.csv(paste0("../results/scenario1/eICU/icd10/los__",x,"__",minsup_border,"__counter_set","/part-00000"))
      colnames(counter_set) <- "LOS"
      counter_set$LOS <- as.integer(counter_set$LOS)
      counter_set <- counter_set[sample(nrow(counter_set), nrow(counter_set),replace = TRUE), ]
      counter_set <- counter_set %>% as_tibble() %>% rename(LOS = value)
      counter_set <- counter_set %>% group_by(LOS) %>% count()
      counter_set$rel_counter_set <- counter_set$n / sum(counter_set$n)
      
      distribution <- inner_join(set, counter_set, by="LOS")
      plot_data <- distribution %>% select(LOS, rel, rel_counter_set)
      
      kld <- round(sum(distribution$rel_counter_set * log(distribution$rel_counter_set/distribution$rel)),4)
      kld_vec <- append(kld_vec, kld)
    }
    kld_sd <- round(sd(kld_vec), 2)
    print(paste0("Error bars scenario 1 los_", x, "__", minsup_border, ": ", kld_sd))
  }
}




top_n <- 2
minsup_borders <- c("0.025" ,"0.05", "0.075")


for (minsup_border in minsup_borders){
  for (x in 1:top_n) {
    set <- read.csv(paste0("../results/scenario1/eICU/icd10/los__",x,"__",minsup_border,"__set","/part-00000"))
    colnames(set) <- "LOS"
    set$LOS <- as.integer(set$LOS)
    print(paste0("Mean scenario 1 los_", x, "__", minsup_border, ": ", set$LOS %>% mean()))
    print(paste0("SD scenario 1 los_", x, "__", minsup_border, ": ", set$LOS %>% sd()))
    print(paste0("IQR scenario 1 los_", x, "__", minsup_border, ": ", set$LOS %>% IQR()))
    print(paste0("SUP scenario 1 los_", x, "__", minsup_border, ": ", set$LOS %>% length()))
    print(paste0(""))
  }
}






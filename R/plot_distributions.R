churn_for_bank_customers_df <- read.csv("../data/churn_for_bank_customers/churn_for_bank_customers.csv")
churn_for_bank_customers_df <- churn_for_bank_customers_df %>% 
  group_by(CreditScore) %>% 
  count()

pdf(paste0("../results/churn_for_bank_customers_distribution.pdf"), width = 22, height = 12)
print(ggplot(churn_for_bank_customers_df, aes(x=CreditScore, y=n)) + 
  geom_bar(stat="identity", alpha=.4, color="steelblue", fill="steelblue") +
  labs(x = "credit score", y = "n") +
  theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) +
  guides(fill="none"))
dev.off()



telco_customer_churn_df <- read.csv("../data/telco_customer_churn/telco_customer_churn.csv")
telco_customer_churn_df <- telco_customer_churn_df %>% 
  group_by(tenure) %>% 
  count()

pdf(paste0("../results/telco_customer_churn_distribution.pdf"), width = 22, height = 12)
print(ggplot(telco_customer_churn_df, aes(x=tenure, y=n)) + 
        geom_bar(stat="identity", alpha=1., color="steelblue", fill="steelblue") +
        labs(x = "tenure", y = "n") +
        theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) +
        guides(fill="none"))
dev.off()



all_patients_df <- read.csv("../data/mimic/all_patients.csv") %>% filter(hospital_expire_flag == 0)
all_patients_df <- all_patients_df %>% 
  distinct(hadm_id, icu_los) %>% 
  group_by(icu_los) %>% 
  count()

pdf(paste0("../results/mimic_distribution.pdf"), width = 22, height = 12)
print(ggplot(all_patients_df, aes(x=icu_los, y=n)) + 
        geom_bar(stat="identity", alpha=1., color="steelblue", fill="steelblue") +
        labs(x = "ICU LOS", y = "n") +
        theme(plot.title = element_text(hjust = 0.5), text = element_text(size=45)) +
        guides(fill="none"))
dev.off()

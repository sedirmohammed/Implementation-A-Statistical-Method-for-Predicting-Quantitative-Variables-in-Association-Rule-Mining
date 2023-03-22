
admissions <- read.csv("../data/raw/mimic/admissions.csv") %>% 
  filter(hospital_expire_flag == 0) %>% 
  select(hadm_id, admission_type, insurance, hospital_expire_flag)

admissions_type_DF <- admissions %>%
  group_by(admission_type) %>% 
  count() %>% 
  tibble::rowid_to_column("admission_type_id") %>% 
  select(-n)

admissions <- admissions %>% 
  inner_join(admissions_type_DF, by = "admission_type") %>% 
  select(-admission_type)

insurance_DF <- admissions %>%
  group_by(insurance) %>% 
  count() %>% 
  tibble::rowid_to_column("insurance_id") %>% 
  select(-n)

admissions <- admissions %>% 
  inner_join(insurance_DF, by = "insurance") %>% 
  select(-insurance)


diagnoses_icd <- read.csv("../data/raw/mimic/diagnoses_icd.csv") %>% 
  select(hadm_id, icd_code, icd_version)
colnames(diagnoses_icd) <- c("hadm_id", "icd_code_dia", "icd_version")
diagnoses_icd$icd_version <- as.numeric(diagnoses_icd$icd_version)
diagnoses_icd <- diagnoses_icd %>% 
  select(-icd_version)

diagnoses_icd_DF <- diagnoses_icd %>%
  group_by(icd_code_dia) %>% 
  count() %>% 
  tibble::rowid_to_column("icd_code_dia_id") %>% 
  select(-n)

diagnoses_icd <- diagnoses_icd %>% 
  inner_join(diagnoses_icd_DF, by = "icd_code_dia") %>% 
  select(-icd_code_dia)
  


procedures_icd <- read.csv("../data/raw/mimic/procedures_icd.csv") %>% 
  select(hadm_id, icd_code, icd_version)
colnames(procedures_icd) <- c("hadm_id", "icd_code_pro", "icd_version")
procedures_icd$icd_version <- as.numeric(procedures_icd$icd_version)
procedures_icd <- procedures_icd %>% 
  select(-icd_version)

procedures_icd_DF <- procedures_icd %>%
  group_by(icd_code_pro) %>% 
  count() %>% 
  tibble::rowid_to_column("icd_code_pro_id") %>% 
  select(-n)

procedures_icd <- procedures_icd %>% 
  inner_join(procedures_icd_DF, by = "icd_code_pro") %>% 
  select(-icd_code_pro)


icustays <- read.csv("../data/raw/mimic/icustays.csv") %>% 
  select(subject_id, hadm_id, los, intime)
icustays$intime <- anytime(icustays$intime)
colnames(icustays) <- c("subject_id", "hadm_id", "icu_los", "intime")
icustays <- icustays %>% 
  group_by(hadm_id) %>% 
  mutate(min_values = min(intime)) %>%
  ungroup()
icustays <- icustays %>% 
  filter(intime == min_values) %>% 
  select(-min_values, -intime)
icustays <- icustays %>% 
  rowwise() %>% 
  mutate(icu_los = floor(icu_los)) %>% 
  as.data.frame()


patients <- read.csv("../data/raw/mimic/patients.csv") %>% 
  select(subject_id, gender)

patients <- patients %>% 
  rowwise() %>% 
  mutate(gender = case_when(gender == "F" ~ 0, gender == "M" ~ 1))



all_patients <- icustays %>% 
  inner_join(patients, by="subject_id") %>% select(-subject_id)
all_patients <- all_patients %>% inner_join(admissions, by="hadm_id") %>% filter(hospital_expire_flag == 0)
all_patients <- all_patients %>% inner_join(diagnoses_icd, by="hadm_id")
all_patients <- all_patients %>% inner_join(procedures_icd, by="hadm_id")


fwrite(all_patients, "../data/mimic/all_patients.csv", row.names=TRUE)
fwrite(admissions_type_DF, "../data/mimic/admissions_type_DF.csv", row.names=TRUE)
fwrite(diagnoses_icd_DF, "../data/mimic/diagnoses_icd_DF.csv", row.names=TRUE)
fwrite(procedures_icd_DF, "../data/mimic/procedures_icd_DF.csv", row.names=TRUE)
fwrite(insurance_DF, "../data/mimic/insurance_DF.csv", row.names=TRUE)


temp <- all_patients %>%
  select(hadm_id, insurance_id) %>% 
  distinct(hadm_id, insurance_id)
temp %>% 
  group_by(insurance_id) %>%
  summarise(perc_diff = round((n()/nrow(temp))*100, 0))

temp <- all_patients %>%
  select(hadm_id, admission_type_id) %>% 
  distinct(hadm_id, admission_type_id)
temp %>% 
  group_by(admission_type_id) %>%
  summarise(perc_diff = round((n()/nrow(temp))*100, 0))


temp <- all_patients %>%
  select(hadm_id, gender) %>% 
  distinct(hadm_id, gender)
temp %>% 
  group_by(gender) %>%
  summarise(perc_diff = round((n()/nrow(temp))*100, 0))


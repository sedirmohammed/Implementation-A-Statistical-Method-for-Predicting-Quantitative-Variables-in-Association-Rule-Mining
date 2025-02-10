import math
import pandas as pd


def process_icd_codes(df, icd_column, code_type):
    df[f'{code_type}code_split'] = df[icd_column].str.split(',')
    df_expanded = df.explode(f'{code_type}code_split')
    df_expanded[f'{code_type}code_split'] = df_expanded[f'{code_type}code_split'].str.strip()
    if code_type == 'icd10':
        regex = r'^[A-Za-z]'
    else:
        regex = r'^\d'
    df_only = df_expanded[df_expanded[f'{code_type}code_split'].str.match(regex)]
    df_only = df_only.drop(columns=[icd_column]).rename(columns={f'{code_type}code_split': f'{code_type}code'})
    df_only[f'{code_type}code'] = df_only[f'{code_type}code'].str.replace('.', '', regex=False).astype(str)

    unique_codes = df_only[f'{code_type}code'].unique()
    mapping = pd.DataFrame({
        'icd_code_dia_id': range(1, len(unique_codes) + 1),
        'icd_code_dia': unique_codes
    })
    mapping_dict = mapping.set_index('icd_code_dia')['icd_code_dia_id'].to_dict()
    df_only['icd_code_dia_id'] = df_only[f'{code_type}code'].map(mapping_dict)
    df_only = df_only.drop(columns=[f'{code_type}code'])
    return df_only.drop_duplicates(), mapping


df_patients = pd.read_csv('../data/eICU/raw/patient.csv')
df_patients = df_patients[
    ['patientunitstayid', 'gender', 'hospitaladmitsource', 'hospitaladmitoffset', 'unitdischargeoffset',
     'hospitaldischargestatus', 'age', 'uniquepid']].dropna()

admission_type_mapping = {
    'Direct Admit': 'URGENT',  
    'Emergency Department': 'EW EMER.', 
    'Floor': 'OBSERVATION ADMIT',  
    'Operating Room': 'SURGICAL SAME DAY ADMISSION',
    'Other Hospital': 'URGENT',  
    'Other ICU': 'URGENT',
    'ICU to SDU': 'OBSERVATION ADMIT',  
    'Step-Down Unit (SDU)': 'OBSERVATION ADMIT',
    'Recovery Room': 'SURGICAL SAME DAY ADMISSION',  
    'Chest Pain Center': 'EW EMER.',  
    'Acute Care/Floor': 'URGENT',  
    'PACU': 'SURGICAL SAME DAY ADMISSION',  
    'Observation': 'DIRECT OBSERVATION',
    'ICU': 'URGENT',  
    'Other': 'DIRECT OBSERVATION'  
}

admission_type_id_mapping = {
    'AMBULATORY OBSERVATION': 1,
    'DIRECT EMER.': 2,
    'DIRECT OBSERVATION': 3,
    'ELECTIVE': 4,
    'EU OBSERVATION': 5,
    'EW EMER.': 6,
    'OBSERVATION ADMIT': 7,
    'SURGICAL SAME DAY ADMISSION': 8,
    'URGENT': 9
}

df_patients['hospitaladmitsource'] = df_patients['hospitaladmitsource'].map(admission_type_mapping)
df_patients['admission_type_id'] = df_patients['hospitaladmitsource'].map(admission_type_id_mapping)
#df_patients.drop('hospitaladmitsource', axis=1, inplace=True)
df_patients = df_patients[df_patients['hospitaldischargestatus'] == 'Alive'].drop('hospitaldischargestatus', axis=1)
df_patients['icu_los'] = ((df_patients['unitdischargeoffset'] - df_patients['hospitaladmitoffset']) / 1440).apply(math.ceil)
df_patients = df_patients[df_patients['icu_los'] >= 0]
df_patients = df_patients.drop(columns=['unitdischargeoffset', 'hospitaladmitoffset'])
df_patients['gender'] = df_patients['gender'].map({'Female': 0, 'Male': 1})
df_patients = df_patients.dropna(subset=['gender'])
df_patients['gender'] = df_patients['gender'].astype(int)

df_patients['age'] = df_patients['age'].replace('> 89', 90)
df_patients['age'] = df_patients['age'].astype(int)
df_patients = df_patients[df_patients['age'] >= 18]
df_patients = df_patients.drop(columns=['age'])


patientunitstayid_count = df_patients['patientunitstayid'].nunique()


df_diagnosis = pd.read_csv('../data/eICU/raw/diagnosis.csv')
df_diagnosis = df_diagnosis[['patientunitstayid', 'icd9code']].dropna()

df_icd9_only, icd9_mapping = process_icd_codes(df_diagnosis.copy(), 'icd9code', 'icd9')
df_icd10_only, icd10_mapping = process_icd_codes(df_diagnosis.copy(), 'icd9code', 'icd10')

df_patients_icd9 = df_patients.merge(df_icd9_only, how='inner', on='patientunitstayid')
df_patients_icd10 = df_patients.merge(df_icd10_only, how='inner', on='patientunitstayid')

patientunitstayid_count = df_patients_icd10['patientunitstayid'].nunique()
uniquepid_count = df_patients_icd10['uniquepid'].nunique()


df_patients_icd9.to_csv('../data/eICU/patients_icd9.csv', index=False)
df_patients_icd10.to_csv('../data/eICU/patients_icd10.csv', index=False)
icd9_mapping.to_csv('../data/eICU/icd9_mapping.csv', index=False)
icd10_mapping.to_csv('../data/eICU/icd10_mapping.csv', index=False)

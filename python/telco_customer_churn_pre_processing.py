from sklearn.datasets import fetch_openml


df = fetch_openml(name='telco-customer-churn', as_frame=True, parser='pandas')
df = df.frame

# Keep rows where customer has churned to know how many month they have been a customer
df = df[df['Churn'] == 'Yes']

df = df.drop(columns=['TotalCharges', 'MonthlyCharges', 'Churn'])
df = df.dropna(axis='rows')

for column in df.columns:
    if column != 'tenure':
        df[column] = df[column].astype(str) + '_' + column

print(df.columns)
print(df)
# write data to csv
df.to_csv('../data/telco_customer_churn/telco_customer_churn.csv', index=False, header=True)

# calculate unique values per column and calculate ratio of values
for column in df.columns:
    if column != 'tenure':
        print(column)
        for value in df[column].unique():
            print(f'{value}: {round((df[column].value_counts()[value] / df.shape[0])*100, 0)}')
        print()


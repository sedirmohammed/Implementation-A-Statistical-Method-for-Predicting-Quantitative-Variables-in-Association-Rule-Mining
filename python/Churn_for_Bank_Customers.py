from sklearn.datasets import fetch_openml


df = fetch_openml(name='Churn-for-Bank-Customers', as_frame=True, parser='pandas')

# ames as pandas dataframe
df = df.frame

# Delete all int columns except age
df = df.drop(columns=['RowNumber', 'CustomerId', 'Surname', 'Age', 'Tenure', 'Balance', 'EstimatedSalary'])

# drop columns with missing values
df = df.dropna(axis='columns')
df = df[df['Exited'] == 0]
df = df.drop(columns=['Exited'])

for column in df.columns:
    if column != 'CreditScore':
        df[column] = df[column].astype(str) + '_' + column

# write data to csv
df.to_csv('../data/churn_for_bank_customers/churn_for_bank_customers.csv', index=False, header=True)

# calculate unique values per column and calculate ratio of values
for column in df.columns:
    if column != 'CreditScore':
        print(column)
        for value in df[column].unique():
            print(f'{value}: {round((df[column].value_counts()[value] / df.shape[0])*100, 0)}')
        print()

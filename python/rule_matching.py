import os
from collections import Counter
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def parse_rules(file_path):
    rules = {}
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split(';')
            left_side = parts[0]
            if 'E-' in parts[1]:
                parts[1] = parts[1].replace(',', ';', 1)
                new_parts = parts[1].strip().split(';')
                parts[1] = new_parts[0]
                parts.append(new_parts[1])
            kld_sorting = float(parts[1])
            icu_los = list(map(int, parts[2].split(',')))
            rules[left_side] = {'kld_sorting': kld_sorting, 'icu_los': icu_los}
    return rules

def get_top_rules_by_kld_sorting(rules, top_percent):
    sorted_rules = sorted(rules.items(), key=lambda item: item[1]['kld_sorting'], reverse=True)
    num_to_keep = int(len(sorted_rules) * (top_percent / 100))
    top_rules = dict(sorted_rules[:num_to_keep])
    return top_rules


def calculate_kld(distribution1, distribution2):
    def calculate_probabilities(arr):
        counter = Counter(arr)
        total = len(arr)
        return {k: v / total for k, v in counter.items()}

    prob1 = calculate_probabilities(distribution1)
    prob2 = calculate_probabilities(distribution2)

    unique_values = set(prob1.keys()).union(set(prob2.keys()))
    aligned_prob1 = np.array([prob1.get(k, 0) for k in unique_values])
    aligned_prob2 = np.array([prob2.get(k, 0) for k in unique_values])

    mask = aligned_prob1 > 0
    epsilon = 1e-10
    aligned_prob1 = aligned_prob1 + epsilon
    aligned_prob2 = aligned_prob2 + epsilon
    kld = np.sum(aligned_prob1[mask] * np.log(aligned_prob1[mask] / aligned_prob2[mask]))
    return kld

def reduce_icd_hierarchy(rule, max_depth):
    items = rule.split(',')
    reduced_items = []
    for item in items:
        if '_diagnose' in item:
            base, suffix = item.split('_', 1)
            if len(base) > 3:
                truncated = base[:3]
                if max_depth > 0:
                    truncated += base[3:3 + max_depth]
                reduced_items.append(f"{truncated}_{suffix}")
            else:
                reduced_items.append(item)
        else:
            reduced_items.append(item)
    return ','.join(reduced_items)

def reduce_rules_hierarchy(rules, max_depth):
    reduced_rules = {}
    for rule, content in rules.items():
        reduced_rule = reduce_icd_hierarchy(rule, max_depth)
        if reduced_rule in reduced_rules:
            reduced_rules[reduced_rule]['icu_los'].extend(content['icu_los'])
        else:
            reduced_rules[reduced_rule] = content
    return reduced_rules

def match_rules_and_calculate_kld(mimic_rules, eicu_rules):
    matched_results = []

    for left_side in mimic_rules:
        if left_side in eicu_rules:
            mimic_los = mimic_rules[left_side]['icu_los']
            eicu_los = eicu_rules[left_side]['icu_los']
            kld_value_forward = calculate_kld(mimic_los, eicu_los)
            kld_value_reverse = calculate_kld(eicu_los, mimic_los)
            kld_value = 0.5 * (kld_value_forward + kld_value_reverse)
            matched_results.append({
                'rule': left_side,
                'mimic_kld_sorting': mimic_rules[left_side]['kld_sorting'],
                'eicu_kld_sorting': eicu_rules[left_side]['kld_sorting'],
                'kld': kld_value
            })

    return matched_results

def process_rules_for_top_percent(minsup_values, mimic_folder, eicu_folder, max_depth, top_percent):
    kld_data = []

    for minsup in minsup_values:
        print(f"Processing minsup: {minsup}")
        mimic_path = os.path.join(mimic_folder, f"all_rules__{minsup}", "part-00000")
        eicu_icd9_path = os.path.join(eicu_folder, 'icd9', f"all_rules__{minsup}", "part-00000")
        eicu_icd10_path = os.path.join(eicu_folder, 'icd10', f"all_rules__{minsup}", "part-00000")

        mimic_rules = parse_rules(mimic_path)
        eicu_icd9_rules = parse_rules(eicu_icd9_path)
        eicu_icd10_rules = parse_rules(eicu_icd10_path)

        eicu_icd9_rules = get_top_rules_by_kld_sorting(eicu_icd9_rules, top_percent)
        print(f"eICU ICD-9 rules: {len(eicu_icd9_rules)}")
        eicu_icd10_rules = get_top_rules_by_kld_sorting(eicu_icd10_rules, top_percent)
        print(f"eICU ICD-10 rules: {len(eicu_icd10_rules)}")
        eicu_rules = {**eicu_icd9_rules, **eicu_icd10_rules}

        mimic_rules = get_top_rules_by_kld_sorting(mimic_rules, top_percent)
        print(f"MIMIC-IV rules: {len(mimic_rules)}")

        matched_results = match_rules_and_calculate_kld(mimic_rules, eicu_rules)
        print(f"Matched rules: {len(matched_results)}")

        kld_values = [match['kld'] for match in matched_results]
        kld_data.extend([(minsup, top_percent, kld) for kld in kld_values])
        print('#####################\n')
    return kld_data

def plot_combined_boxplots(kld_data):
    df = pd.DataFrame(kld_data, columns=['Minsup', 'TopPercent', 'KLD'])
    df['TopPercent'] = df['TopPercent'].astype(str) + '%'

    plt.figure(figsize=(10, 7))
    sns.boxplot(x='Minsup', y='KLD', hue='TopPercent', data=df, palette='Set2')
    plt.xlabel('Minsup', fontsize=20)
    plt.ylabel('KLD', fontsize=20)
    plt.legend(title='Top % of Rules', fontsize=15, title_fontsize=18)
    plt.yticks(fontsize=15)
    plt.xticks([0, 1, 2], ['2.5%', '5%', '7.5%'], fontsize=15)
    plt.tight_layout()
    plt.show()

def plot_combined_violinplots(kld_data):
    df = pd.DataFrame(kld_data, columns=['Minsup', 'TopPercent', 'KLD'])
    df['TopPercent'] = df['TopPercent'].astype(str) + '%'

    plt.figure(figsize=(10, 7))
    sns.violinplot(x='Minsup', y='KLD', hue='TopPercent', data=df, palette='Set2', split=True)
    plt.xlabel('Minsup', fontsize=20)
    plt.ylabel('KLD', fontsize=20)
    plt.legend(title='Top % of Rules', fontsize=15, title_fontsize=18)
    plt.yticks(fontsize=15)
    plt.xticks([0, 1, 2], ['2.5%', '5%', '7.5%'], fontsize=15)
    plt.tight_layout()
    plt.show()
def plot_maxdepth_boxplot(all_kld_data):
    data = []
    for max_depth, kld_data in all_kld_data.items():
        for minsup, kld in kld_data:
            data.append({'MaxDepth': max_depth, 'Minsup': minsup, 'KLD': kld})

    df = pd.DataFrame(data)
    df['MaxDepth'] = pd.Categorical(df['MaxDepth'], categories=sorted(all_kld_data.keys(), reverse=True), ordered=True)

    for minsup in df['Minsup'].unique():
        sns.boxplot(x='MaxDepth', y='KLD', data=df[df['Minsup'] == minsup])
        plt.title(f'KLD Distribution by MaxDepth for Minsup {minsup}')
        plt.xlabel('MaxDepth')
        plt.ylabel('KLD')
        plt.show()

def main():
    minsup_values = [0.025, 0.05, 0.075]
    mimic_folder = '../results/scenario1/mimic_reduced'
    eicu_folder = '../results/scenario1/eICU'

    all_kld_data = []

    for max_depth in [3]:
        print(f"\nProcessing with max depth: {max_depth}\n")

        kld_data_top_40 = process_rules_for_top_percent(minsup_values, mimic_folder, eicu_folder, max_depth, top_percent=40)

        kld_data_full = process_rules_for_top_percent(minsup_values, mimic_folder, eicu_folder, max_depth, top_percent=100)

        for minsup in minsup_values:
            kld_values = [kld for minsup_val, top_percent, kld in kld_data_full if minsup_val == minsup]
            print(f'Minsup: {minsup}, SD: {np.std(kld_values)}, Mean: {np.mean(kld_values)}')

        for minsup in minsup_values:
            kld_values = [kld for minsup_val, top_percent, kld in kld_data_top_40 if minsup_val == minsup]
            print(f'Minsup: {minsup}, SD: {np.std(kld_values)}, Mean: {np.mean(kld_values)}')

        all_kld_data.extend(kld_data_full)
        all_kld_data.extend(kld_data_top_40)

    plot_combined_boxplots(all_kld_data)
    #plot_combined_violinplots(all_kld_data)


if __name__ == "__main__":
    main()

import matplotlib.pyplot as plt

minsups = [0.025, 0.05, 0.075]
rules_mimic = [256, 58, 24]
rules_eicu = [46, 17, 9]
matched_rules = [25, 12, 5]

bar_width = 0.2
x = range(len(minsups))

percentages = [
    matched_rules[i] / min(rules_mimic[i], rules_eicu[i]) * 100 for i in range(len(minsups))
]

plt.figure(figsize=(10, 5))

plt.bar([p - bar_width for p in x], rules_mimic, width=bar_width, label='#Rules MIMIC-IV')
plt.bar(x, rules_eicu, width=bar_width, label='#Rules eICU')
bars_matched = plt.bar([p + bar_width for p in x], matched_rules, width=bar_width, label='#Matched Rules')

for bar, perc in zip(bars_matched, percentages):
    plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 10, f'{perc:.1f}%',
             ha='center', va='bottom', fontsize=14, color='black')

plt.xticks(x, [f'{m*100}%' for m in minsups], fontsize=17)
plt.yticks(fontsize=20)
plt.xlabel('Minsup', fontsize=20)
plt.ylabel('Number of Rules', fontsize=20)
plt.legend(fontsize=20)

plt.tight_layout()
plt.show()

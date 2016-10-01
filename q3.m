fontsize = 14;
q3X = {'MR';'Tez'};
q3Time = [350.54 397.255 362.034; 367.505 460.101 372.375];
bar(q3Time);
set(gca,'XTickLabel',q3X, 'FontSize', fontsize-4);
xlabel('Method and % of completion', 'FontSize', fontsize);
ylabel('Completion Time (s)', 'FontSize', fontsize);
legend({'null','25%', '75%'}, 'Location','best', 'FontSize', fontsize)

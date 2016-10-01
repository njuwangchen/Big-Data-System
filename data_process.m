clear all;
close all;
clc;

fontsize = 14;
%% Q1.a completion time
% filepath = 'result/1/';
% % completion time
% suffix = '_query_completion_time';
% filename_mr = [filepath,'mr',suffix];
% filename_tez = [filepath,'tez',suffix];
% 
% % mr
% fformat = '%d: %f';
% fmr = fopen(filename_mr);
% mrDataTmp = textscan(fmr, fformat);
% fclose(fmr);
% mrData = [mrDataTmp{1} mrDataTmp{2}];
% mrData = sortrows(mrData, 1);
% 
% 
% % tez
% ftez = fopen(filename_tez);
% tezDataTmp = textscan(ftez, fformat);
% tezData = [tezDataTmp{1} t: ezDataTmp{2}];
% tezData = sortrows(tezData, 1);

% % plot
% bar([mrData(:,2) tezData(:,2)]);
% set(gca,'XTickLabel',strcat('query ', num2str(mrData(:,1))), 'FontSize', fontsize-4);
% xlabel('Query', 'FontSize', fontsize);
% ylabel('Completion Time (s)', 'FontSize', fontsize);
% legend({'Mapreduce','Tez'}, 'Location','northwest', 'FontSize', fontsize)

%%Q1.b disk and bandwidth
% % disk 
% filename_mr = [filepath, 'MR_disk'];
% filename_tez = [filepath, 'Tez_disk'];
% 
% % mr
% fformat = '%f %f %f';
% fmr = fopen(filename_mr);
% mrDataTmp = textscan(fmr, fformat);
% fclose(fmr);
% mrData = [mrDataTmp{1} mrDataTmp{2}.*0.5./1024/1024 mrDataTmp{3}.*0.5./1024/1024];
% mrData = sortrows(mrData, 1)
% 
% 
% % tez
% ftez = fopen(filename_tez);
% tezDataTmp = textscan(ftez, fformat);
% tezData = [tezDataTmp{1} tezDataTmp{2}.*0.5./1024/1024 tezDataTmp{3}.*0.5./1024/1024];
% tezData = sortrows(tezData, 1)
% 
% % plot
% bar([mrData(:,2:3) tezData(:,2:3)]);
% set(gca,'XTickLabel',strcat('query ', num2str(mrData(:,1))), 'FontSize', fontsize-4);
% xlabel('Query', 'FontSize', fontsize);
% ylabel('Amount of Data (GB)', 'FontSize', fontsize);
% legend({'Mapreduce Read','Mapreduce Write', 'Tez Read', 'Tez Write'}, 'Location','northwest', 'FontSize', fontsize)

% % bandwidth
% filename_mr_net = [filepath, 'MR_disk'];
% filename_tez_net = [filepath, 'Tez_disk'];
% 
% % mr
% fformat = '%f %f %f';
% fmr_net = fopen(filename_mr_net);
% mrDataTmp_net = textscan(fmr_net, fformat);
% fclose(fmr_net);
% mrData_net = [mrDataTmp_net{1} mrDataTmp_net{2}./1024./1024./double(mrData(:,2)) mrDataTmp_net{3}./1024./1024./double(mrData(:,2))];
% mrData_net = sortrows(mrData_net, 1)
% 
% 
% % tez
% ftez_net = fopen(filename_tez_net);
% tezDataTmp_net = textscan(ftez_net, fformat);
% fclose(ftez_net);
% tezData_net = [tezDataTmp_net{1} tezDataTmp_net{2}./1024./1024./double(tezData(:,2)) tezDataTmp_net{3}./1024./1024./double(tezData(:,2))];
% tezData_net = sortrows(tezData_net, 1)
% 
% % plot
% bar([mrData_net(:,2:3) tezData_net(:,2:3)]);
% set(gca,'XTickLabel',strcat('query ', num2str(mrData_net(:,1))), 'FontSize', fontsize-4);
% xlabel('Query', 'FontSize', fontsize);
% ylabel('Amount of Data (GB/s)', 'FontSize', fontsize);
% legend({'Mapreduce Rx','Mapreduce Tx', 'Tez Rx', 'Tez Tx'}, 'Location','northwest', 'FontSize', fontsize)

%% Q1.c History

% file read
filepath_mr = 'mr_history/';
filepath_tez = 'tez_history/';

tasks = [];

mrHist = {};
tezHist = {};
id = 1;
for i = [12 21 50 71 85]
    filename_mr = [filepath_mr,num2str(i),'/',num2str(i)];
    filename_tez = [filepath_tez,num2str(i),'/',num2str(i)];
    
    % mr
    fformat = '%s %s %s %s %u64 %s %u64 %s %s';
    fmr = fopen(filename_mr);
    tmp = textscan(fgets(fmr),'%s');
    totalTaskMR = str2num(tmp{1}{2});
    tmp = textscan(fgets(fmr),'%s');
    totalMapperMR = str2num(tmp{1}{2});
    tmp = textscan(fgets(fmr),'%s');
    totalReducerMR = str2num(tmp{1}{2});
    
    mrDataTmp = textscan(fmr, fformat, 'Delimiter',{':',' '});
    fclose(fmr);
    % seperate mapper and reducer
    %distimr = parseTasks(mrDataTmp{5}, mrDataTmp{7}, mrDataTmp{3});
    distimr = parseTasks(mrDataTmp{5}, mrDataTmp{7});
    
    
    % tez
    fformat = '%s %s %u64 %s %u64 %s %s';
    ftez = fopen(filename_tez);
    tmp = textscan(fgets(ftez),'%s');
    totalTasktez = str2num(tmp{1}{2});
   
    tezDataTmp = textscan(ftez, fformat, 'Delimiter',{':',' '});
    fclose(ftez);
    
    distitez = parseTasks(tezDataTmp{3}, tezDataTmp{5});
    
    
    tasks = [tasks; [totalTaskMR, totalMapperMR, totalReducerMR, totalTasktez]];
    
    % plot
    
    timeRange = max(length(distimr), length(distitez));
    if (length(distimr) >= length(distitez))
        distitez = [distitez zeros(1,abs(length(distimr)-length(distitez)))];
    else
        distimr = [distimr zeros(1,abs(length(distimr)-length(distitez)))];
    end
    mrHist(id) = {distimr};
    tezHist(id) = {distitez};
    figure;
    plot(1/1000:1/1000:timeRange/1000, [distimr' distitez']);
    set(gca,'FontSize', fontsize-4);
    xlabel('Time (s)', 'FontSize', fontsize);
    ylabel('Number of Tasks', 'FontSize', fontsize);
    legend({'Mapreduce','Tez'}, 'Location','northeast', 'FontSize', fontsize);

    
    
   id = id + 1; 
end

save q1c;
%% Q2.1 
% 
% filepath = 'result/2/';
% % completion time
% suffix = '_result';
% filename_mr = [filepath,'mr',suffix];
% filename_tez = [filepath,'tez',suffix];
% 
% % mr
% fformat = '%s';
% fmr = fopen(filename_mr);
% mrDataTmp = textscan(fmr, fformat, 'Delimiter', '\n');
% fclose(fmr);
% mrData = [];
% for i = 1:length(mrDataTmp{1})
%     stri = mrDataTmp{1}{i};
%     strSplit = strsplit(stri,{' ', ':', '\n'});
%     i
%     if (length(strSplit) == 13)
%         tmpRes = [];
%         for j = 2:2:12
%             tmpRes = [tmpRes str2num(strSplit{j})];
%         end
%         mrData = [mrData; tmpRes];
%     end
% end
% mrData = sortrows(mrData, [1 2 3 4 5]);
% indices = [1 4 6 11 14 17 20 23 26 29 32 37 40 43 44];
% mrDataAve = [];
% for i = 1:length(indices)-1
%     mrDataAve = [mrDataAve; [mrData(indices(i), 1:4) mean( mrData(indices(i):(indices(i+1)-1),6) )]];
% end
% varReducer = [1 5 10 20; mrDataAve(3,5) mrDataAve(11:13,5)'];
% varShuffle = [5 10 15 20; [mrDataAve(3,5) mrDataAve(8:10,5)']];
% varSlow = [0.05 0.25 0.50 0.75 1.00; mrDataAve(4:8,5)'];
% 
% % plot var reducer
% % bar(varReducer(2,:));
% % set(gca,'XTickLabel',num2str(varReducer(1,:)), 'FontSize', fontsize-4);
% % xlabel('# Reducers', 'FontSize', fontsize);
% % ylabel('Mapreduce Completion Time (s)', 'FontSize', fontsize);
% %legend({'Mapreduce Rx','Mapreduce Tx', 'Tez Rx', 'Tez Tx'}, 'Location','northwest', 'FontSize', fontsize)
% 
% % tez
% ftez = fopen(filename_tez);
% tezDataTmp = textscan(ftez, fformat, 'Delimiter', '\n');
% fclose(ftez);
% tezData = [];
% for i = 1:length(tezDataTmp{1})
%     stri = tezDataTm% 
% filepath = 'result/2/';
% % completion time
% suffix = '_result';
% filename_mr = [filepath,'mr',suffix];
% filename_tez = [filepath,'tez',suffix];
% 
% % mr
% fformat = '%s';
% fmr = fopen(filename_mr);
% mrDataTmp = textscan(fmr, fformat, 'Delimiter', '\n');
% fclose(fmr);
% mrData = [];
% for i = 1:length(mrDataTmp{1})
%     stri = mrDataTmp{1}{i};
%     strSplit = strsplit(stri,{' ', ':', '\n'});
%     i
%     if (length(strSplit) == 13)
%         tmpRes = [];
%         for j = 2:2:12
%             tmpRes = [tmpRes str2num(strSplit{j})];
%         end
%         mrData = [mrData; tmpRes];
%     end
% end
% mrData = sortrows(mrData, [1 2 3 4 5]);
% indices = [1 4 6 11 14 17 20 23 26 29 32 37 40 43 44];
% mrDataAve = [];
% for i = 1:length(indices)-1
%     mrDataAve = [mrDataAve; [mrData(indices(i), 1:4) mean( mrData(indices(i):(indices(i+1)-1),6) )]];
% end
% varReducer = [1 5 10 20; mrDataAve(3,5) mrDataAve(11:13,5)'];
% varShuffle = [5 10 15 20; [mrDataAve(3,5) mrDataAve(8:10,5)']];
% varSlow = [0.05 0.25 0.50 0.75 1.00; mrDataAve(4:8,5)'];
% 
% % plot var reducer
% % bar(varReducer(2,:));
% % set(gca,'XTickLabel',num2str(varReducer(1,:)), 'FontSize', fontsize-4);
% % xlabel('# Reducers', 'FontSize', fontsize);
% % ylabel('Mapreduce Completion Time (s)', 'FontSize', fontsize);
% %legend({'Mapreduce Rx','Mapreduce Tx', 'Tez Rx', 'Tez Tx'}, 'Location','northwest', 'FontSize', fontsize)
% 
% % tez
% ftez = fopen(filename_tez);
% tezDataTmp = textscan(ftez, fformat, 'Delimiter', '\n');
% fclose(ftez);
% tezData = [];
% for i = 1:length(tezDataTmp{1})
%     stri = tezDataTmp{1}{i};
%     strSplit = strsplit(stri,{' ', ':', '\n'});
%     i
%     if (length(strSplit) == 11)
%         tmpRes = [];
%         for j = 2:2:10
%             tmpRes = [tmpRes str2num(strSplit{j})];
%         end
%         tezData = [tezData; tmpRes];
%     end
% end
% tezData = sortrows(tezData, [1 2 3 4]);
% indices = [1 4 5 8 11 14 17 20 21 22 25 26];
% tezDataAve = [];
% for i = 1:length(indices)-1
%     tezDataAve = [tezDataAve; [tezData(indices(i), 1:3) mean( tezData(indices(i):(indices(i+1)-1),5) )]];
% end
% varshuffle_tez = [5 10 15 20; tezDataAve(4:7,4)'];
% 
% save result;

%     strSplit = strsplit(stri,{' ', ':', '\n'});
%     i
%     if (length(strSplit) == 11)
%         tmpRes = [];
%         for j = 2:2:10
%             tmpRes = [tmpRes str2num(strSplit{j})];
%         end
%         tezData = [tezData; tmpRes];
%     end
% end
% tezData = sortrows(tezData, [1 2 3 4]);
% indices = [1 4 5 8 11 14 17 20 21 22 25 26];
% tezDataAve = [];
% for i = 1:length(indices)-1
%     tezDataAve = [tezDataAve; [tezData(indices(i), 1:3) mean( tezData(indices(i):(indices(i+1)-1),5) )]];
% end
% varshuffle_tez = [5 10 15 20; tezDataAve(4:7,4)'];
% 
% save result;

%var = reducer








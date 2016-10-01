function [distribution] = parseTasks(startT, endT, varargin)

minStart = min(startT) - 1;
startT = startT - minStart;
endT = endT - minStart;
maxT = max(endT);
if nargin == 3
    distribution = zeros(2, maxT);
    for i = 1:length(startT)
        if (strcmp(varargin{1}{i},'MAP'))
            for j = startT(i):endT(i)
                distribution(1,j) = distribution(1,j) + 1;
            end
        else
            for j = startT(i):endT(i)
                distribution(2,j) = distribution(2,j) + 1;
            end
        end
    end
else
    distribution = zeros(1, maxT);
    for i = 1:length(startT)
        for j = startT(i):endT(i)
            distribution(1,j) = distribution(1,j) + 1;
        end
    end
end

end

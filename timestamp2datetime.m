%% timestamp2datetime.m
% This function builds the datetime in the MySQL format YYYY-MM-DD from
% timestamp.

% INPUT:
%         ts: timestamp

% OUTPUT:
%         t2d_err: error flag (0 = correct, 1 = error)
%         datetime: string for datetime in the MySQL format YYYY-MM-DD


% Author: Lorenzo Corgnati
% Date: July 18, 2018

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

function [t2d_err,datetime] = timestamp2datetime(ts)

disp(['[' datestr(now) '] - - ' 'timestamp2datetime.m started.']);

t2d_err = 0;

warning('off', 'all');

% Retrieve the numeric expression of the timestamp
try
    tsCell = strsplit(ts);
    tsVec = [];
    for tsCell_idx=1:length(tsCell)
        tsVec = [tsVec str2double(tsCell{tsCell_idx})];
    end
    tsNum = datenum(tsVec);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    t2d_err = 1;
    return
end

% Build the string for datetime in the MySQL format YYYY-MM-DD
formatOut='yyyy-mm-dd';
datetime = datestr(tsNum,formatOut);

return


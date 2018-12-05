%% OFFLINE_EU_HFR_Node_Processor.m
% This wrapper launches in OFFLINE MODE the scripts for inserting into the HFR database
% the information about radial and total files (both Codar and WERA) and
% for combining radials into totals and converting radials and totals to
% netCDF files according to the European standard data model.

% The offline mode forces the value of the NRT_processed_flag in the
% radial_input_tb and total_input_tb to 0 in order to allow repeated
% processing of the same datasets.

% No infinite loop is active.

% Author: Lorenzo Corgnati
% Date: October 26, 2018

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

warning('off', 'all');

clear all
close all
clc

% Setup netCDF toolbox
setup_nctoolbox;

% Setup JBDC driver for MySQL
javaaddpath('/Users/reverendo/Toolboxes/mysql-connector-java-5.1.17.jar');

% Setup map colormap
set(0,'DefaultFigureColormap',feval('jet'));

EHNP_err = 0;

disp(['[' datestr(now) '] - - ' 'EU_HFR_Node_Processor started.']);

%%

%% Set HFR provider username

HFRPusername = 'lorenzo';

%%

%% Set database parameters NOT TO BE CHANGED

sqlConfig.user = 'HFR_lorenzo';
sqlConfig.password = 'xWeLXHFQfvpBmDYO';
sqlConfig.host = '150.145.136.8';
sqlConfig.database = 'HFR_node_db';

%%

%% Set datetime of the starting date of the processing period

try
    startDate = startCombinationDate(now);
    startDate = '2012-12-31';
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    HFRC_err = 1;
end

%%

%% Forces the value of the NRT_processed_flag in radial_input_tb and total_input_tb to 0

% Connect to database
try
    conn = database(sqlConfig.database,sqlConfig.user,sqlConfig.password,'Vendor','MySQL','Server',sqlConfig.host);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    EHNP_err = 1;
end
if(EHNP_err==0)
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully established.']);
end

% Query the database for retrieving the networks managed by the HFR provider username
% Set and exectute the query
try
    HFRPusername_selectquery = ['SELECT network_id FROM account_tb WHERE username = ' '''' HFRPusername ''''];
    HFRPusername_curs = exec(conn,HFRPusername_selectquery);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    EHNP_err = 1;
end
if(EHNP_err==0)
    disp(['[' datestr(now) '] - - ' 'Query to account_tb table successfully executed.']);
end
% Fetch data
try
    HFRPusername_curs = fetch(HFRPusername_curs);
    HFRPusername_data = HFRPusername_curs.Data;
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    EHNP_err = 1;
end
if(EHNP_err==0)
    disp(['[' datestr(now) '] - - ' 'Data from account_tb table successfully fetched.']);
end
% Close cursor
try
    close(HFRPusername_curs);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    EHNP_err = 1;
end
if(EHNP_err==0)
    disp(['[' datestr(now) '] - - ' 'Cursor to account_tb table successfully closed.']);
end

% Retrieve networks ID managed by the HFR provider username
try
    HFRPnetworks = regexp(HFRPusername_data{1}, '[ ,;]+', 'split');
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    EHNP_err = 1;
end

% Query the database for setting NRT_processed_flag to 0 for managed networks
if(EHNP_err==0)
    % Set and exectute the query for radial_input_tb
    try
        reset_updatequeryR = 'UPDATE radial_input_tb SET NRT_processed_flag=0 WHERE network_id = ''';
        for HFRPntw_idx=1:length(HFRPnetworks)-1
            reset_updatequeryR = [reset_updatequeryR HFRPnetworks{HFRPntw_idx} ''' OR network_id = ' ''''];
        end
        reset_updatequeryR = [reset_updatequeryR HFRPnetworks{length(HFRPnetworks)} ''''];
        reset_radial_curs = exec(conn,reset_updatequeryR);
    catch err
        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
        EHNP_err = 1;
    end
    if(EHNP_err==0)
        disp(['[' datestr(now) '] - - ' 'Query to radial_input_tb table successfully executed.']);
    end
    
    % Set and exectute the query for total_input_tb
    try
        reset_updatequeryT = 'UPDATE `total_input_tb` SET `NRT_processed_flag`=0 WHERE network_id = ''';
        for HFRPntw_idx=1:length(HFRPnetworks)-1
            reset_updatequeryT = [reset_updatequeryT HFRPnetworks{HFRPntw_idx} ''' OR network_id = ' ''''];
        end
        reset_updatequeryT = [reset_updatequeryT HFRPnetworks{length(HFRPnetworks)} ''''];
        reset_total_curs = exec(conn,reset_updatequeryT);
    catch err
        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
        EHNP_err = 1;
    end
    if(EHNP_err==0)
        disp(['[' datestr(now) '] - - ' 'Query to total_input_tb table successfully executed.']);
    end
    
    % Close cursors
    try
        close(reset_radial_curs);
        close(reset_total_curs);
    catch err
        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
        EHNP_err = 1;
    end
    if(EHNP_err==0)
        disp(['[' datestr(now) '] - - ' 'Cursors to radial_input_tb and total_input_tb table successfully closed.']);
    end
end

%%

%% Processing

% RADIALS COMBINATION & RADIALS AND TOTALS CONVERSION
%      inputRUV2DB;
%      inputCradAscii2DB;
HFRCombiner;

% TOTALS CONVERSION
%     inputTUV2DB;
%     inputCurAsc2DB;
%     TotalConversion;

disp(['[' datestr(now) '] - - ' 'OFFLINE_EU_HFR_Node_Processor run ended.']);
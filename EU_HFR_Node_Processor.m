%% EU_HFR_Node_Processor.m
% This wrapper launches the scripts for inserting into the HFR database 
% the information about radial and totala files (both Codar and WERA) and
% for combining radials into totals and converting radials and totals to
% netCDF files according to the European standard data model.

% Author: Lorenzo Corgnati
% Date: October 20, 2018

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

%% Set database parameters

sqlConfig.user = 'HFR_lorenzo';
sqlConfig.password = 'xWeLXHFQfvpBmDYO';
sqlConfig.host = '150.145.136.8';
sqlConfig.database = 'HFR_node_db';

%%

% Set the infinite loop for continuous operation
kk = 5;
while(kk>0)
    % RADIALS COMBINATION & RADIALS AND TOTALS CONVERSION
    inputRUV2DB;
    inputCradAscii2DB;
    HFRCombiner;
    
    % RADIALS COMBINATION & RADIALS AND TOTALS CONVERSION
    inputTUV2DB;
    inputCurAsc2DB;
    TotalConversion;
    
    disp(['[' datestr(now) '] - - ' 'EU_HFR_Node_Processor loop ended.']);
end
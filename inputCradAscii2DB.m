%% inputCradAscii2DB.m
% This application lists the input crad_ascii (WERA radials) files pushed by the HFR data providers
% and insert into the HFR database the information needed for the
% combination of radial files into totals and for the generation of the
% radial and total data files into the European standard data model.

% Author: Lorenzo Corgnati
% Date: October 3, 2018

% E-mail: lorenzo.corgnati@sp.ismar.cnr.it
%%

warning('off', 'all');

iCradDB_err = 0;

disp(['[' datestr(now) '] - - ' 'inputCradAscii2DB started.']);

startDateNum = datenum(startDate);

%%

%% Connect to database

try
    conn = database(sqlConfig.database,sqlConfig.user,sqlConfig.password,'Vendor','MySQL','Server',sqlConfig.host);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully established.']);
end

%%

%% Query the database for retrieving the networks managed by the HFR provider username

% Set and exectute the query
try
    HFRPusername_selectquery = ['SELECT network_id FROM account_tb WHERE username = ' '''' HFRPusername ''''];
    HFRPusername_curs = exec(conn,HFRPusername_selectquery);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Query to account_tb table for retrieving the networks managed by the HFR provider username successfully executed.']);
end

% Fetch data
try
    HFRPusername_curs = fetch(HFRPusername_curs);
    HFRPusername_data = HFRPusername_curs.Data;
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Data of the networks managed by the HFR provider username successfully fetched from account_tb table.']);
end

% Close cursor
try
    close(HFRPusername_curs);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Cursor to account_tb table successfully closed.']);
end

%%

%% Retrieve networks ID managed by the HFR provider username

try
    HFRPnetworks = regexp(HFRPusername_data{1}, '[ ,;]+', 'split');
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end

%%

%% Query the database for retrieving data from managed networks

% Set and exectute the query
try
    network_selectquery = 'SELECT * FROM network_tb WHERE (network_id = ''';
    for HFRPntw_idx=1:length(HFRPnetworks)-1
        network_selectquery = [network_selectquery HFRPnetworks{HFRPntw_idx} ''' OR network_id = ' ''''];
    end
    network_selectquery = [network_selectquery HFRPnetworks{length(HFRPnetworks)} ''') AND EU_HFR_processing_flag=0'];
    network_curs = exec(conn,network_selectquery);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Query to network_tb table for retrieving data of the managed networks successfully executed.']);
end

% Fetch data
try
    network_curs = fetch(network_curs);
    network_data = network_curs.Data;
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Data of the managed networks successfully fetched from network_tb table.']);
end

% Retrieve column names
try
    network_columnNames = columnnames(network_curs,true);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Column names from network_tb table successfully retrieved.']);
end

% Retrieve the number of networks
try
    numNetworks = rows(network_curs);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Number of managed networks successfully retrieved from network_tb table.']);
end

% Close cursor
try
    close(network_curs);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Cursor to network_tb table successfully closed.']);
end

%%

%% Scan the networks, find the stations, list the related radial files and insert information into the database

try
    % Find the index of the network_id field
    network_idIndexC = strfind(network_columnNames, 'network_id');
    network_idIndex = find(not(cellfun('isempty', network_idIndexC)));
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end

% Scan the networks
try
    for network_idx=1:numNetworks
        try
            station_selectquery = ['SELECT * FROM station_tb WHERE network_id = ' '''' network_data{network_idx,network_idIndex} ''''];
            station_curs = exec(conn,station_selectquery);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iCradDB_err = 1;
        end
        if(iCradDB_err==0)
            disp(['[' datestr(now) '] - - ' 'Query to station_tb table for retrieving the stations of the ' network_data{network_idx,network_idIndex} ' network successfully executed.']);
        end
        
        % Fetch data
        try
            station_curs = fetch(station_curs);
            station_data = station_curs.Data;
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iCradDB_err = 1;
        end
        if(iCradDB_err==0)
            disp(['[' datestr(now) '] - - ' 'Data of the stations of the ' network_data{network_idx,network_idIndex} ' network successfully fetched from station_tb table.']);
        end
        
        % Retrieve column names
        try
            station_columnNames = columnnames(station_curs,true);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iCradDB_err = 1;
        end
        if(iCradDB_err==0)
            disp(['[' datestr(now) '] - - ' 'Column names from station_tb table successfully retrieved.']);
        end
        
        % Retrieve the number of stations belonging to the current network
        try
            numStations = rows(station_curs);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iCradDB_err = 1;
        end
        if(iCradDB_err==0)
            disp(['[' datestr(now) '] - - ' 'Number of stations belonging to the ' network_data{network_idx,network_idIndex} ' network successfully retrieved from station_tb table.']);
        end
        
        % Close cursor to station_tb table
        try
            close(station_curs);
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iCradDB_err = 1;
        end
        if(iCradDB_err==0)
            disp(['[' datestr(now) '] - - ' 'Cursor to station_tb table successfully closed.']);
        end
        
        try
            % Find the index of the input file path field
            inputPathIndexC = strfind(station_columnNames, 'radial_input_folder_path');
            inputPathIndex = find(not(cellfun('isempty', inputPathIndexC)));
            
            % Find the index of the station_id field
            station_idIndexC = strfind(station_columnNames, 'station_id');
            station_idIndex = find(not(cellfun('isempty', station_idIndexC)));
        catch err
            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
            iCradDB_err = 1;
        end
        
        % Scan the stations
        for station_idx=1:numStations
            if(~isempty(station_data{station_idx,inputPathIndex}))
                % Trim heading and trailing whitespaces from folder path
                station_data{station_idx,inputPathIndex} = strtrim(station_data{station_idx,inputPathIndex});
                % List the input crad_ascii files for the current station
                try
                    cradFiles = rdir([station_data{station_idx,inputPathIndex} filesep '**' filesep '*.crad_ascii'],'datenum>floor(startDateNum)');
                    disp(['[' datestr(now) '] - - ' 'Radial files from ' station_data{station_idx,station_idIndex} ' station successfully listed.']);
                catch err
                    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                    iCradDB_err = 1;
                end
                
                % Insert information about the crad_ascii file into the database (if not yet present)
                for crad_idx=1:length(cradFiles)
                    % Retrieve the filename
                    [pathstr,name,ext]=fileparts(cradFiles(crad_idx).name);
                    noFullPathName=[name ext];
                    % Check if the current crad_ascii file is already present on the database
                    try
                        dbRadials_selectquery = ['SELECT * FROM radial_input_tb WHERE datetime>' '''' startDate ''' AND network_id = ' '''' network_data{network_idx,network_idIndex} ''' AND filename = ' '''' noFullPathName ''' ORDER BY timestamp'];
                        dbRadials_curs = exec(conn,dbRadials_selectquery);
                    catch err
                        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                        iCradDB_err = 1;
                    end
                    if(iCradDB_err==0)
                        disp(['[' datestr(now) '] - - ' 'Query to radial_input_tb table for checking if ' noFullPathName ' radial file is already present in the database successfully executed.']);
                    end
                    % Fetch data
                    try
                        dbRadials_curs = fetch(dbRadials_curs);
                    catch err
                        disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                        iCradDB_err = 1;
                    end
                    if(iCradDB_err==0)
                        disp(['[' datestr(now) '] - - ' 'Data about the presence of ' noFullPathName ' radial file in the database successfully fetched from radial_input_tb table.']);
                    end
                    
                    if(rows(dbRadials_curs) == 0)
                        % Retrieve information about the crad_ascii file
                        try
%                             % Load the total file as text
%                             cradFile = textread(cradFiles(crad_idx).name,  '%s', 'whitespace', '\n');
%                             % Read the file header and look for timestamp
%                             for line_idx=1:length(cradFile)
%                                 splitLine = regexp(cradFile{line_idx}, '[ \t]+', 'split');
%                                 if(length(splitLine)>1)
%                                     expressionDate = '([0-9]{2}-[A-Z]{3}-[0-9]{2})';
%                                     expressionTime = '([0-9]{2}:[0-9]{2})';
%                                     expressionUTC = 'UTC';
%                                     [startIndexDate,endIndexDate] = regexp(cradFile{line_idx},expressionDate);
%                                     [startIndexTime,endIndexTime] = regexp(cradFile{line_idx},expressionTime);
%                                     [startIndexUTC,endIndexUTC] = regexp(cradFile{line_idx},expressionUTC);
%                                     if((~isempty(startIndexDate)) && (~isempty(startIndexTime)) && (~isempty(startIndexUTC)))
%                                         date = cradFile{line_idx}(startIndexDate:endIndexDate);
%                                         time = cradFile{line_idx}(startIndexTime:endIndexTime);
%                                         TimeStampVec = datevec([date ' ' time]);
%                                         TimeStamp = [num2str(TimeStampVec(1)) ' ' num2str(TimeStampVec(2),'%02d') ' ' num2str(TimeStampVec(3),'%02d') ' ' num2str(TimeStampVec(4),'%02d') ' ' num2str(TimeStampVec(5),'%02d') ' ' num2str(TimeStampVec(6),'%02d')];
%                                         break;
%                                     end
%                                 end
%                             end
                            % Read the timestamp from the header
                            [date,time] = textread(cradFiles(crad_idx).name, '%*15c %9c %*0c %5c',1);
                            TimeStampVec = datevec([date ' ' time]);
                            TimeStamp = [num2str(TimeStampVec(1)) ' ' num2str(TimeStampVec(2),'%02d') ' ' num2str(TimeStampVec(3),'%02d') ' ' num2str(TimeStampVec(4),'%02d') ' ' num2str(TimeStampVec(5),'%02d') ' ' num2str(TimeStampVec(6),'%02d')];
                        catch err
                            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                            iCradDB_err = 1;
                        end
                        
                        try
                            % Evaluate datetime from, Time Stamp
                            [t2d_err,DateTime] = timestamp2datetime(TimeStamp);
                        catch err
                            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                            iCradDB_err = 1;
                        end
                        
                        % Retrieve information about the crad_ascii file
                        try
                            cradFileInfo = dir(cradFiles(crad_idx).name);
                            cradFilesize = cradFileInfo.bytes/1024;
                        catch err
                            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                            iCradDB_err = 1;
                        end
                        
                        % Write crad_ascii info in radial_input_tb table
                        try
                            % Define a cell array containing the column names to be added
                            addColnames = {'filename' 'filepath' 'network_id' 'station_id' 'timestamp' 'datetime' 'reception_date' 'filesize' 'extension' 'NRT_processed_flag'};
                            
                            % Define a cell array that contains the data for insertion
                            addData = {noFullPathName,pathstr,network_data{network_idx,network_idIndex},station_data{station_idx,station_idIndex},TimeStamp,DateTime,(datestr(now,'yyyy-mm-dd HH:MM:SS')),cradFilesize,'crad_ascii',0};
                            
                            % Append the product data into the radial_input_tb table on the database.
                            tablename = 'radial_input_tb';
                            datainsert(conn,tablename,addColnames,addData);
                        catch err
                            disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
                            iCradDB_err = 1;
                        end
                        if(iCradDB_err==0)
                            disp(['[' datestr(now) '] - - ' noFullPathName ' radial file information successfully inserted into radial_input_tb table.']);
                        end
                    end
                end
            end
        end
    end
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end

%%

%% Close connection

try
    close(conn);
catch err
    disp(['[' datestr(now) '] - - ERROR in ' mfilename ' -> ' err.message]);
    iCradDB_err = 1;
end
if(iCradDB_err==0)
    disp(['[' datestr(now) '] - - ' 'Connection to database successfully closed.']);
end

%%

disp(['[' datestr(now) '] - - ' 'inputCradAscii2DB successfully executed.']);
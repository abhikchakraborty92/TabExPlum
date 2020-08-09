import tableauserverclient as TSC
import pandas as pd
import datetime
import time
import os
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
from tableauhyperapi import escape_string_literal
import json
from tableauserverclient import server
from tableauserverclient.models import tableau_auth


# Console
def consolelog(console_string:str):
    print(f"\n{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:\t{console_string}")

def stringclean(stringval):
    enemyvals = f"!@#$%^&*(){{}};:',./\\`~?+-| "
    stringval = ['_' if char in enemyvals else char for char in stringval]
    stringval = ''.join(stringval)
    return stringval


print('\n')
print('#'*20+ ' TABLEAU EXPRESS PLUMBER '+ '#'*20)

def login(username=None,password=None,svr=None,email=None,siteurl=None,credential_path='credential.json'):
    if credential_path is not None:
        try:
            # Reading credentials from the json file
            credentials = open(credential_path).read()
            credentials = json.loads(credentials)
            
            # Setting Up Tableau Server Connection credentials from the credential file
            username =  credentials.get('username')
            password =  credentials.get('password')
            svr = credentials.get('server')
            email = credentials.get('email')
            siteurl = credentials.get('sitename')
        except Exception as e:
            return consolelog(f'Credentials could not be read because {e}')
            exit()
    else:
        if username is None or password is None or svr is None or email is None:
            return consolelog(f'Incomplete credentials provided')
            exit()

    try:
        consolelog('Signing in...')
        
        #Signing in to the Server
        tableau_auth = TSC.TableauAuth(username=username, password=password,site=siteurl)
        server = TSC.Server(svr)
        server.use_server_version()

        # Adding a filter for user related records
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.OwnerEmail,
                                        TSC.RequestOptions.Operator.Equals,
                                        email))
        server.auth.sign_in(tableau_auth)
        consolelog('Signed in!')
    except Exception as e:
        consolelog(f'Login failed because {e}')
        exit()
    
    time.sleep(2)
    return (tableau_auth,server,req_option)

server_creds = login()
tableau_auth = server_creds[0]
server = server_creds[1]
req_option = server_creds[2]

def getviewdata(all_items):

    """
    Internal function to be used to generate dataframe with details of user published views in the server

    Args:
        all_items ([tableau item object]): Accepts item object of a specific item_type

    Returns:
        [Dataframe]: Dataframe with the details
    """

    consolelog('Fetching View Data...')
    itemlist= []
    for item in all_items:
        itemtuple = (item.name,item.id,item.workbook_id)
        itemlist.append(itemtuple)
    
    itemtable = pd.DataFrame(itemlist,columns=['Item Name','Item ID','Workbook ID'])

    consolelog('View data fetch completed')
    return itemtable


def getprojectdata(all_items):
    """
    Internal function to be used to generate dataframe with details of only public projects in the server

    Args:
        all_items ([tableau item object]): Accepts item object of a specific item_type

    Returns:
        [Dataframe]: Dataframe with the details
    """

    consolelog('Fetching Projects Data...')
    itemlist= []
    for item in all_items:
        itemtuple = (item.name,item.id)
        itemlist.append(itemtuple)
    
    itemtable = pd.DataFrame(itemlist,columns=['Item Name','Item ID'])

    consolelog('Projects Data fetch completed')
    return itemtable

def getdatasourcedata(all_items):

    """
    Internal function to be used to generate dataframe with details of user published datasources in the server

    Args:
        all_items ([tableau item object]): Accepts item object of a specific item_type

    Returns:
        [Dataframe]: Dataframe with the details
    """

    consolelog('Fetching Data Sources Data...')
    itemlist= []
    for item in all_items:
        itemtuple = (item.name,item.id,item.created_at,item.updated_at,item.project_name)
        itemlist.append(itemtuple)
    
    itemtable = pd.DataFrame(itemlist,columns=['Item Name','Item ID','Creation Date','Update Date','Project Name'])

    consolelog('Data Sources Data fetch completed')
    return itemtable

def getworkbookdata(all_items):
    """
    Internal function to be used to generate dataframe with details of user published workbooks in the server

    Args:
        all_items ([tableau item object]): Accepts item object of a specific item_type

    Returns:
        [Dataframe]: Dataframe with the details
    """


    consolelog('Fetching Workbook Data...')
    itemlist= []
    for item in all_items:
        server.workbooks.populate_views(item)
        for view_detail in item.views:
            itemtuple = (item.name,item.id,view_detail.name,view_detail.id)
            itemlist.append(itemtuple)
    
    itemtable = pd.DataFrame(itemlist,columns=['Item Name','Item ID','View Name','View ID'])

    consolelog('Workbook Data fetch completed')
    return itemtable

def getitemdetails(item_type=None,conditions=req_option):

    """
    Use this function to generate a dataframe containing details of the selected item_type. The item_type here can be PROJECT, WORKBOOK, DATASOURCE, VIEW

    Args:
    item_type ([string], optional): Specify the item type here. This can be 'Project','View','Workbook' or 'Datasource'. Defaults to None.

    Returns:
        [Dataframe]
    """

    consolelog(f'Data requested for the tableau server item: {item_type}')
    time.sleep(1)
    itemtable = pd.DataFrame()
    consolelog('Fetching data...')
    with server.auth.sign_in(tableau_auth):
        try:
            if item_type.lower() == 'project':
                all_items, pagination_item = server.projects.get()
                itemtable = getprojectdata(all_items)
            elif item_type.lower() == 'workbook':
                all_items, pagination_item = server.workbooks.get(conditions)
                itemtable = getworkbookdata(all_items)
            elif item_type.lower() == 'view':
                all_items, pagination_item = server.views.get(conditions)
                itemtable = getviewdata(all_items)
            elif item_type.lower() == 'datasource':
                all_items, pagination_item = server.datasources.get(conditions)
                itemtable = getdatasourcedata(all_items)
            else:
                return itemtable
        except Exception as e:
            return consolelog(f'Operation failed because {e}')
    
    consolelog('Data Fetch completed')
    return itemtable


def get_item_obj(item_type=None,search_string=None,conditions=req_option):
    """
    [summary]
    
    This function can be used to return an object from the tableau server based on a search string. The type of the object can be PROJECT, WORKBOOK, DATASOURCE, VIEW.
    Please note that for PROJECT, the objects are the one which are available publicaly on the server for now

    Args:
    item_type ([string], optional): Specify the item type here. This can be 'Project','View','Workbook' or 'Datasource'. Defaults to None.

    search_string ([string], optional): The string that will be used to find the specific item object from the tableau server. Defaults to None.
    
    conditions ([type], optional): This function argument can be ignored

    Returns:
        [type]: [description]
    """


    item_obj = None

    with server.auth.sign_in(tableau_auth):
        if search_string is None:
            return None
        else:
            if item_type.lower() == 'view':
                all_items,pagination_item = server.views.get(conditions)
            elif item_type.lower() == 'workbook':
                all_items,pagination_item = server.workbooks.get(conditions)
            elif item_type.lower() == 'project':
                all_items,pagination_item = server.projects.get()
            elif item_type.lower() == 'datasource':
                all_items,pagination_item = server.datasources.get(conditions)
            elif item_type.lower() == 'job':
                all_items,pagination_item = server.jobs.get(conditions)
            else:
                return None

        consolelog(f'Search String: {search_string.upper()} | Checking {item_type.upper()} items...')
        for item in all_items:
            if item.name == search_string:
                item_obj = item
                
    return item_obj


def get_directory(workbookname,filepath,filename):

    """
    [summary]
    This resolves the directory using the filepath variable from the downloadview function. This again is an internal function and should not be called directly outside downloadview function

    Returns:
        [type]: [description]
    """

    if filepath == None or filepath == '':
        filepath = os.getcwd()
    
    if os.path.exists(filepath):
        consolelog(f'{filepath} exists. Generating fullpath...')
        filepath = os.path.join(filepath,stringclean(workbookname))
        if os.path.exists(filepath)==False:
            os.mkdir(filepath)

        fullpath = os.path.join(filepath,filename)
    else:
        consolelog(f"{filepath} doesn't exist. Generating fullpath...")
        os.mkdir(filepath)
        filepath = os.path.join(filepath,stringclean(workbookname))
        os.mkdir(filepath)
        fullpath = os.path.join(filepath,filename)
    consolelog(f'Filepath: {fullpath}')
    return fullpath



def getviewmedia(view_obj,filepath,workbookname,viewname,fileformat):
    
    """[summary]
    This is an internal helper function that downloads the view from the workbook. This should not be called outside 'downloadview' function
    """


    consolelog(f'Requesting media for the view: {viewname.upper()} in the workbook: {workbookname.upper()}')
    if fileformat.lower() == 'image':
        try:
            server.views.populate_image(view_obj)
            consolelog('Fetching Directory...')
            filename = f'{stringclean(viewname)}.png'
            fullpath = get_directory(workbookname,filepath,filename) 
            consolelog(f'Writing {fileformat}...')
            with open(fullpath, 'wb') as f:
                f.write(view_obj.image)
                consolelog(f'{fileformat} download completed at {fullpath}')
        except EnvironmentError as e:
            consolelog(f'ERROR: \t {str(e)}')
    elif fileformat.lower() == 'pdf':
        try:
            server.views.populate_pdf(view_obj)
            consolelog('Fetching Directory...')
            filename = f'{stringclean(viewname)}.pdf'
            fullpath = get_directory(workbookname,filepath,filename)
            consolelog(f'Writing {fileformat}...')
            with open(fullpath, 'wb') as f:
                f.write(view_obj.pdf)
                consolelog(f'{fileformat} download completed at {fullpath}')
        except EnvironmentError as e:
            consolelog(f'ERROR: \t {str(e)}')
            return None
    elif fileformat.lower() == 'csv':
        try:
            server.views.populate_csv(view_obj)
            consolelog('Fetching Directory...')
            filename = f'{stringclean(viewname)}.csv'
            fullpath = get_directory(workbookname,filepath,filename)
            consolelog(f'Writing {fileformat}...')
            with open(fullpath, 'wb') as f:
                f.write(b''.join(view_obj.csv))
                consolelog(f'{fileformat} download completed at {fullpath}')
        except EnvironmentError as e:
            consolelog(f'ERROR: \t {str(e)}')
            return None
    else:
        consolelog('File Format Not Supported')
        consolelog('Exiting...')
        time.sleep(2)
        return None



def downloadview(workbookname,viewname=None,conditions=req_option,filepath=None,fileformat=None):

    """
    [summary]
    This is a powerful function that downloads the view/views of a tableau workbook in PDF or PNG format

    Args:
    
    workbookname[string]: Name of the workbook from where the views have to be downloaded
    
    viewname[string]: Name of the view which has to be downloaded. If this is not provided, all the views of the provided workbook would be downloaded

    filepath[string]: Address/Location on the machine where the view has to be downloaded

    """


    time.sleep(2)
    try:
        try:
            workbook_id = get_item_obj(item_type='workbook',search_string=workbookname).id
        except:
            return consolelog('No workbook found. Exiting...')
        consolelog(f'Workbook ID for the workbook is {workbook_id}')
        with server.auth.sign_in(tableau_auth):
            workbook_obj = server.workbooks.get_by_id(workbook_id)
            server.workbooks.populate_views(workbook_obj)
            for view_obj in workbook_obj.views:

                if viewname != None:
                    if view_obj.name == viewname:
                        getviewmedia(view_obj=view_obj,filepath=filepath,workbookname=workbookname,viewname=viewname,fileformat=fileformat)
                    else:
                        consolelog('No view found...')
                        consolelog('Exiting...')
                        time.sleep(1)
                        return None
                else:
                    time.sleep(2)
                    consolelog(f'Downloading {view_obj.name}.{fileformat}')
                    getviewmedia(view_obj=view_obj,filepath=filepath,workbookname=workbookname,viewname=view_obj.name,fileformat=fileformat)

    except EnvironmentError as e:
        consolelog(f'Error: \t {e}')
        time.sleep(1)
        consolelog('Exiting...')
        time.sleep(1)
        return None
    

def convert_datatype(coldatatype):

    """
    [summary]
        This converts the datatype of the column of a given dataframe.
    
    Args:
        Datatype of the column in string format
    
    Returns:
        The tableau hyper extract compatible datatype after converting the dataframe datatype and a default value for NaN cases
    """

    datatype = SqlType.text()
    def_value = ''
    
    if 'datetime' in coldatatype.lower():
        datatype = SqlType.timestamp()
    elif 'str' in coldatatype.lower():
        datatype = SqlType.text()
    elif  'boolean' in coldatatype.lower():
        datatype = SqlType.bool()
    elif  'int' in coldatatype.lower():
        datatype = SqlType.int()
        def_value = 0
    elif 'float' in coldatatype.lower():
        datatype = SqlType.double()
        def_value = 0
    elif 'period' in coldatatype.lower():
        datatype = SqlType.interval()
    elif 'object' in coldatatype.lower():
        datatype = SqlType.text()
    else:
        datatype = SqlType.text()
    
    return (datatype,def_value)
    


def create_extract_schema(raw_data=None,raw_data_path=None):
    """
    [summary]
    This function generates the tableau hyper file columns to be used for generation of the hyper extract file

    Args:
        raw_data ([dataframe], optional): Pass the raw pandas dataframe here. If this is not found, the function will check for raw_file_path. Defaults to None.
        
        raw_data_path ([string], optional): Pass path of the csv file for the dataframe if raw dataframe is not available. Defaults to None.

    Returns:
        [object list]: Returns a list of objects representing tableau hyper extract columns
    """
    if raw_data is None:
        try:
            raw_data = pd.read_csv(raw_data_path)
        except:
            return consolelog('ERROR: No filepath found')
    elif raw_data is not None:
        pass
    else:
        return consolelog('ERROR: No data found')

    columns  = raw_data.columns.tolist()
    tableau_extract_columns = []

    for col in columns:
        conversion_values = convert_datatype(str(raw_data[[col]].dtypes[0]))
        converted_type = conversion_values[0]
        def_value = conversion_values[1] 
        consolelog(f"Column: [{col}] with datatype ~{str(raw_data[[col]].dtypes[0])}~ converted to {converted_type}")
        tableau_extract_columns.append(TableDefinition.Column(col,converted_type))
        raw_data[col] = raw_data[col].fillna(def_value)
    
    return (tableau_extract_columns,raw_data)


def create_tableau_extract(extract_path,raw_data_path=None,raw_data=None,custom_schema=False,schema_path=None):

    """[summary]
        This function creates the Tableau extract hyper file from a provided dataframe.

        [function variables]
        raw_data_path = Full path of the CSV file including the extension. Example: C:\\File Location\\Myrawdata.csv. This is an optional field if raw_data is provided

        extract_path = Full path of the Tableau Extract file including the Extract file name and the location where the file will be stored. Example: C:\\File Location\\Myextract.hyper

        raw_data = Pandas dataframe using which extract has to be created
        
        custom_schema = If schema is manually provided then set this as True. By default it is False

        schema_path = If schema is manually provided add the schema path here

    """
    if custom_schema == False:
        if raw_data is not None:
            schema_values = create_extract_schema(raw_data=raw_data)
            schema_columns = schema_values[0]
            raw_data.to_csv('temp.csv',index=False)
            raw_data_path = 'temp.csv'
        elif raw_data is None:
            try:
                schema_values = create_extract_schema(raw_data_path=raw_data_path)
                schema_columns = schema_values[0]
            except:
                return consolelog('ERROR: No filepath found')
        else:
            return consolelog('ERROR: No data found')
    elif custom_schema == True:
        try:
            schema_columns = open(schema_path).read()
            schema_columns = list(schema_columns)
        except:
            return consolelog('ERROR: Schema file unreadable')
    else:
        return consolelog('ERROR: Schema invalid. Extract cannot be created')

    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'ac_hyper_app' ) as hyper:
        
        with Connection(endpoint=hyper.endpoint, 
                    create_mode=CreateMode.CREATE_AND_REPLACE,
                    database=extract_path) as connection:

                    connection.catalog.create_schema('Extract')

                    schema = TableDefinition(table_name=TableName('Extract','Extract'),
                    columns=schema_columns)

                    connection.catalog.create_table(schema)


                    # with Inserter(connection, schema) as inserter:
                    #         for index, row in raw_data.iterrows():
                    #             inserter.add_row(row)
                            
                    #         inserter.execute()
                    #         return consolelog('Extract file has been generated')

                    extract_row_count = connection.execute_command(
            command=f"COPY {schema.table_name} FROM {escape_string_literal(raw_data_path)} WITH "
            f"(format csv, NULL 'NULL', delimiter ',', header)"
            )       
                    try:
                        os.remove('temp.csv')
                    except:
                        pass
                    return consolelog(f'Extract file has been generated with {extract_row_count} rows') 

def refresh_tableau_data(source_name):
    """[summary]

    Args:
        source_name ([string]): Name of the tableau source to be refreshed
    
    Returns:
        [type]: [Job Object to be queried later]
    """


    data_source_obj = get_item_obj(item_type='datasource',search_string=source_name)

    if data_source_obj is not None:
        with server.auth.sign_in(tableau_auth):
            try:
                results = server.datasources.refresh(data_source_obj)
                time.sleep(1)
                consolelog(f"{results.id} | {results.created_at} | {results.completed_at} | {results.finish_code}")
                return results
            except Exception as e:
                return consolelog(f"Source refresh failed because {str(e)}")
    else:
        return consolelog('No datasources found. Please enter a valid source name')


def publish_tableau_data(project_name,extract_file_path,data_source_name=None,write_mode='CreateNew'):
    """[summary]
    
    This function is used for publishing a tableau extract file as a data source to the tableau server
    
    Args:
        
    project_name ([string]): This is the project folder where the tableau data source will be published
    
    extract_file_path ([string]): Location of the tableau extract file
    
    data_source_name ([string], optional): The name to be given to the published tableau data source on the tableau server. Defaults to None.
    
    write_mode (str, optional): A datasource can be modified in a number of ways. This parameter decides what has to be done to the tableau data source.
    
    Values:

    [CreateNew] creates a new source if the source doesn't exist. Otherwise it overwrites the source
        
    [Overwrite] Overwrites an existing source if the source exists. Otherwise it creates a new source

    [Append] Appends to an existing source

            
    Defaults to 'CreateNew'.

    Returns:
        
        [string]: Returns a console message if the data source is successfully created or failed
    """


    if project_name is None:
        return consolelog('ERROR: No project specified')
    else:
        try:
            project_id = get_item_obj(item_type='project',search_string=project_name).id
        except:
            return consolelog('ERROR: No project found')


    if data_source_name is not None:
        try:
            if get_item_obj(item_type='datasource',search_string=data_source_name) is not None:
                if write_mode == 'CreateNew':
                    write_mode = 'Overwrite'
            else:
                write_mode = 'CreateNew'  
            new_datasource = TSC.DatasourceItem(project_id,name=data_source_name)
            consolelog('New datasource item created')
        except:
            return consolelog('New datasource item creation failed')
    else:
        new_datasource = TSC.DatasourceItem(project_id)
    with server.auth.sign_in(tableau_auth):
        try:
            new_datasource = server.datasources.publish(
                            new_datasource, extract_file_path, write_mode)
            return consolelog('Data Source has been successfully published')
        except Exception as e:
            return consolelog(f'Data Source publishing failed because {e}')

def delete_tableau_data(source_name):
    """[summary]

    Args:
        source_name ([string]): Name of the tableau source to be deleted
    
    Returns:
        [type]: [Job Object to be queried later]
    """


    data_source_obj = get_item_obj(item_type='datasource',search_string=source_name)
    consolelog(f'{data_source_obj.name} | {data_source_obj.id}')
    if data_source_obj is not None:
        with server.auth.sign_in(tableau_auth):
            try:
                results = server.datasources.delete(data_source_obj.id)
                time.sleep(1)
                consolelog(f"Data source has been deleted successfully")
                return results
            except Exception as e:
                return consolelog(f"Source deletion failed because {str(e)}")
    else:
        return consolelog('No datasources found. Please enter a valid source name')


def refresh_tableau_workbook(workbookname):
    """[summary]
    This function refreshes a tableau workbook
    Args:
        workbookname ([string]): Name of the tableau workbook to be refreshed
    
    Returns:
        [type]: [Job Object to be queried later]
    """


    workbook_obj = get_item_obj(item_type='workbook',search_string=workbookname)

    if workbook_obj is not None:
        with server.auth.sign_in(tableau_auth):
            try:
                results = server.workbooks.refresh(workbook_obj)
                time.sleep(1)
                consolelog(f"Workbook: {results.name.upper()} has been refreshed")
                return results
            except Exception as e:
                return consolelog(f"Source refresh failed because {str(e)}")
    else:
        return consolelog('No workbooks found. Please enter a valid workbook name')

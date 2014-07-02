__author__ = 'Josh Phillips <jap260@georgetown.edu'
__license__ = 'MIT'
__copyright__ = '2014, Georgetown University'
__version__ = "1.0.1"


#Project must be imported as the masterclass for REDCap API connections
from redcap import Project
#connect must be imported from pyodbc to allow SQL strings to be based back to the database
from pyodbc import connect as cxn






'''The following is the list defintion for api_key_list (found below)'''
 ############################################################
'''
    API Key Descriptions:
    index      Name    
    0     =     NTSR LRPR Tracking Dev
    1     =     NTSR Registration dev
    2     =     NTSR Ruesch Tracking Dev
    3     =     NTSR Registration
    4     =     NTSR LRPR Tracking
    5     =     NTSR Ruesch Tracking
''' 
############################################################
api_key_list =['C091B3C3D9D999725A11339A26AF34A9','F51FD90B744137A11A97DF0654F739D0','DFDEF050566A4DB6745C2A4424B149C8','300D9B9652AA755E320CA55E89BF7961','56509384CEF6F8048EB9F2AD2542DC85','94926D47F5B35A9E5861425A249BC3DD']
#url_list[0] is the dev web server url_list[1] is production 
url_list = ['https://redcap-dev.georgetown.edu/redcap/api/', 'https://redcap.georgetown.edu/redcap/api/']
# TEST Vars
#reg = reg()
#recs = reg.export_records()
#conn = cxn('DRIVER={SQL Server}; SERVER=lccc-cpc-sql.uis.georgetown.edu;DATABASE=NTSR_dev')     #Remote connection for NTSR
#conn = cxn('DRIVER={SQL Server}; SERVER=LCCC-CPC-SQL;DATABASE=NTSR_dev')                          #local connection for NTSR
#c = conn.cursor()
#api_key_list =['3326398096F62710CDEF1C6CFC7D387B','F51FD90B744137A11A97DF0654F739D0','DFDEF050566A4DB6745C2A4424B149C8','300D9B9652AA755E320CA55E89BF7961','56509384CEF6F8048EB9F2AD2542DC85','94926D47F5B35A9E5861425A249BC3DD']
#url_list = ['https://redcap-dev.georgetown.edu/redcap/api/', 'https://redcap.georgetown.edu/redcap/api/']

class redcap_connection(object):
    '''Contains properties and values necessary to transform REDCap projects into SQL Database tables
    Required parameters:
            url   --> this should be the url of your redcap database. Usually of the form https://redcap.host/api/
            key  --> this is the API key generated within redcap itself. If you do not have one contact your system administrator for more information about gaining access
            table_name  --> this is the SQL table name you will be inserting into your ODBC SQL database. 
    Optional parameters:
        **WARNING: when this flag is set to True you are susceptible to a man-in the middle attack, use at your own risk**
        dev = True  --> This will not verify ssl and assumes you are working in a dev environment (or one without an up to date ssl certificate)    
        dev = False -->  This setting will require an ssl certificate from the hosted redcap database
            
        records
        forms
        '''
    def __init__(self,  key, url, table_name, connection='', driver='',server='',database='',  dev=False, project='',  records='', forms = []):
        self.connection                      = connection
        self.key                                  = key
        self.url                                  = url
        #self.server                                  = server
        #self.driver                          = driver
        #self.database                           = database
        self.dev                                 = dev
        self.table_name                         = table_name
        self.records                            = records
        self.forms                              = forms
        if self.dev == False:
            self.project                        = Project(self.url, self.key)
            print 'Production Environment'
        if self.dev == True:
            self.project                        = Project(self.url,self.key, verify_ssl=False)
            print 'Dev Environment'
       

            
        
                
            
    def schema(self, participant_id= True):
        '''Processes the REDCap metadata to generate sql schema and creates a table reflecting that schema in your ODBC destination
        PARAMS:
            participant_id:
                    If flagged true (default) it will make the first column in your schema = participant_id [varchar(50)]
        '''
        #Exports metadata for the forms specifed. If no forms are specified all forms are included. 
        self.metadata = self.project.export_metadata(forms=self.forms)
        
        #These if statements check if table name is given and handles the participant_id flag for inclusion of that field in the database
        if self.table_name:
            if participant_id == True:
                #participant ids are handled differently than the other columns because it is typically used as a primary key and should be included on tables created for all forms. 
                participant_id_string = '''[participant_id][varchar](500) PRIMARY KEY, \n'''
                #If the table name already exists in the database it is dropped so the new table may be created
                sqlstring = '''IF EXISTS (
                SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].['''+self.table_name+''']') AND type in (N'U')) 
                DROP TABLE [dbo].[''' + self.table_name + ''']
                CREATE TABLE [dbo].[''' + self.table_name + '''](\n''' + participant_id_string
        #In the case that participant_id is not set to True the same process as above occurs but the participant_id is not added as the first column in the table
        if participant_id != True:
            sqlstring = '''IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].['''+self.table_name+''']') AND type in (N'U')) 
                        DROP TABLE [dbo].[''' + self.table_name + '''] CREATE TABLE [dbo].[''' + self.table_name + '''](\n'''
            print 'participant_id != True'
            return
        
        #Escapes function if table_name not provided
        elif not self.table_name:
            print "Please provide a table name"
            return
    
 
   
        #This for loop reads the metadata and adds the appropriate column name and Microsoft SQL Server 2008 R2 datatype, 
        #these datatypes may require modification if destination server is not SQL Server 2008 or compatible, but that modification should be relatively painless. 
        
        
        #Loop iterations
        #1.) Iterate through all forms specified
        #2.)For each form , iterate through its metadata and process each column based off of field_type
        for form in self.forms:
        #Redcap metadata should be passed to this function as a list of dictionaries
            for dictionary in self.metadata:
                #This if statement verifies that only wanted forms are processed in the schema
                if dictionary['form_name'] == form:
                    #REDCap checkbox fields require that their output be stored as multiple columns, these next few lines create appropriate columns for each checkbox column
                    if dictionary['field_type'] == 'checkbox':
                        ls = []
                        #checkbox choices are split and processed from the string first by \\n which is a newline character to form a ls of items in the form ['1, Native-American', '2, Black']                    
                        ls = dictionary['select_choices_or_calculations'].split('|')
                        for i in ls:
                            #for each choice value the field_name + a choice number are given as strings, comments could be injected into the sql code here if desired as easy reminders of what the checkboxes correlate to
                            #for example sqlstring = sqlstring + '/*' + dictionary['select_choices_or_calculations'] + '*/'
                            sqlstring = sqlstring + '['+ dictionary['field_name'] + '___'+ str(int(i.split(",")[0])) + '] [Float] NULL, \n'
                            
                    #Descriptive fields are ignored as they contain no record information
                    elif dictionary['field_type'] == 'descriptive':
                        continue
                        
                    #This block of elif statements handles date / datetime fields in redcap by calling them as datetime sql 
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'date_ymd':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'date_mdy':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'date_dmy':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'datetime_ymd':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'datetime_mdy':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'datetime_dmy':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'datetime_seconds_ymd':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'datetime_seconds_mdy':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                    elif dictionary['field_type'] == 'text' and dictionary['text_validation_type_or_show_slider_number'] == 'datetime_seconds_dmy':
                        sqlstring = sqlstring + '[' +dictionary['field_name'] + '][datetime] NULL, \n'
                        
                        
                        
                    #This logic codes REDCap text fields as SQL varchar(500) *NOTE* participant_id is handled at instantiation of the function, so it is removed here  
                    elif dictionary['field_type'] == 'text' and dictionary['field_name'] != 'participant_id' and dictionary['text_validation_type_or_show_slider_number'] != 'number' :
                        sqlstring = sqlstring +'[' +dictionary['field_name'] + '][varchar](500) NULL, \n'
                     
                    elif dictionary['field_type'] == 'text' and dictionary['field_name'] != 'participant_id' and dictionary['text_validation_type_or_show_slider_number'] == 'number' :
                        sqlstring = sqlstring +'[' +dictionary['field_name'] + '][float] NULL, \n'    
                    
                   #     
                    elif dictionary['field_type'] == 'dropdown' or dictionary['field_type'] == 'radio' or dictionary['field_type'] == 'calc' :
                        sqlstring = sqlstring +'[' +dictionary['field_name'] + '][float] NULL, \n'
                        
                    elif dictionary['field_type'] == 'notes':
                        sqlstring = sqlstring +'[' +dictionary['field_name'] + '][NVARCHAR](MAX) NULL, \n'
                    
                    elif dictionary['field_name'] == 'participant_id':
                        continue 
                    else:
                        print dictionary['field_name'] + " does not appear to have a coded SQL data type. Please bring this to the attention of the database administrator."
    
        
            
            sqlstring = sqlstring + '[' + form + '_complete' + '] [Float]  NULL, \n'
        #sqlstring[:-3] removes the last 3 characters from the sqlstring which should be ' \n'
        sqlstring = sqlstring[:-3]    
        sqlstring = sqlstring + ''' ) on [PRIMARY]
        '''  
        #Encoding the sqlstring to ASCII helps reduce errors when writing strings to be passed to other programs from python 2.7
        sqlstring = sqlstring.encode('ASCII')                          
        c = cxn(self.connection)
        c = c.cursor()
        c.execute(sqlstring)
        c.commit()
        return #sqlstring
    

 
 
    def insert_records(self, second_entry=False):
        '''Commits a sqlstring to the database and table_name and connection string
        provided''' 
        print "Updating " + self.table_name +" this process may take several minutes. "   
        if not self.table_name:
            print 'A table_name must be specified whose schema matches your recordset'
            return
        
        if not self.connection:
            print 'A connection string must be provided in the format "DRIVER=[SQL Server]; SERVER=[SERVER NAME];DATABASE=[DATABASE NAME]'
            return
        
        #Process: In order to reduce strain on the REDCap server the record export is broken into batches of 10 or less (the last batch may be 1-10)
        #ls is a list placeholder for the master list
        ls = []
        #This for loop downloads the entire list of participant_ids in a REDCap project
        
        
        for id in self.project.export_records(fields=['participant_id']):
            #This try --> except block should ensure that no double entry records are input
            try:
                int(id['participant_id'])
                ls.append(str(id['participant_id']))
            except ValueError:
                continue
        #This next line divides ls into a list of batches (tuple) each containing 10 records (1-10 records for the last list) 
        
        
        divided_list = tuple(ls[i:i+500] for i in xrange(0,len(ls),500))
       
            
        
        
        
        for i in divided_list:
            self.records = self.project.export_records(forms= self.forms, records = i)
            for pid in self.records:
                sqlstring = 'INSERT INTO dbo.' + '[' + self.table_name + ']('
                #print  ' PARTICIPANT ID ---------->  ' + pid['participant_id']
                for field in pid:
                    sqlstring = sqlstring + '[' + field + ']' + ','
                sqlstring = sqlstring[:-1] + ') VALUES('
                for value in pid:
                    if pid[value] != '':
                        sqlstring = sqlstring + "'" + pid[value].replace("'","''")+"'" +','
                    else:
                        sqlstring = sqlstring + 'NULL,'
                sqlstring = sqlstring[:-1] + ')'
                ls.append(sqlstring)
                
                c = self.connection
                c = cxn(c).cursor()
                c.execute(sqlstring)
                c.commit()
                
                
                
            
        
        
        
        

#Connection strings for the project
#
#DRIVER = {SQL SERVER}
#SERVER = lccc-cpc-sql.uis.georgetown.edu    ##Remote
#SERVER = SERVER=LCCC-CPC-SQL                ##Local
#Databases NTSR (prod) NTSR_test (dev)

conn = 'DRIVER={SQL Server}; SERVER=lccc-cpc-sql.uis.georgetown.edu;DATABASE=NTSR'
#conn = 'DRIVER={SQL Server}; SERVER=LCCC-CPC-SQL;DATABASE=NTSR'



    
    
        
        

    
    



#
Reg = redcap_connection(connection = conn, url=url_list[1], key = api_key_list[3], table_name = 'Reg',  forms = ['ntsr_subject_id',  'ntsr_registration', 'ntsr_demographics','ntsr_research_information','ntsr_clinical_and_treatment'])

Reg.schema()
Reg.insert_records()

#
studsurv = redcap_connection(connection = conn, url=url_list[1], key= api_key_list[4], table_name = 'LRPRSubjectAndSurveyTracking',forms=['study_tracking','survey_tracking'])
studsurv.schema()
studsurv.insert_records()

#
biospec = redcap_connection(connection = conn, url=url_list[1], key= api_key_list[4], table_name = 'lrpr_Biospec',forms=['biospecimen_tracking'])
biospec.schema()
biospec.insert_records()

#
triage = redcap_connection(connection = conn, url=url_list[1], key = api_key_list[4], table_name = 'lrpr_Triage', forms=['triage_information'])
triage.schema()
triage.insert_records()

#
specstud = redcap_connection(connection = conn, url=url_list[1], key = api_key_list[4], table_name = 'lrpr_SpecialStudies', forms=['special_biospecimen_studies'])
specstud.schema()
specstud.insert_records()

#
ruesch = redcap_connection(connection = conn, url=url_list[1], key = api_key_list[5], table_name = 'RueschTracking', forms=['study_tracking','survey_tracking','biospecimen_tracking'])
ruesch.schema()
ruesch.insert_records()













    
''' Part 1 Generate Token'''
import base64
import requests
consumerKey = "dzQs3i0GMENUHesVvcV4fOefOdEa"
consumerSecret = "0bf7vqo3fCTLuUY0zuik9SGY17Aa"
keySecret = (consumerKey + ":" + consumerSecret).encode('utf-8')
consumerKeySecretB64 = base64.b64encode(keySecret).decode('utf-8')
tokenResponse = requests.post("https://apistore.datasparkanalytics.com/token",data = { 'grant_type': 'client_credentials'},headers = { 'Authorization': 'Basic ' + consumerKeySecretB64 })
token = tokenResponse.json()['access_token']

''' Part 2 Query API'''
import json
import pandas as pd


# Generate dates for query from 2017-05-01 till competition date 
number31 = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
number30 = number31[:-1]
Jan,Mar,May,Jul,Aug,Oct,Dec = [('01',i) for i in number31],[('03',i) for i in number31],[('05',i) for i in number31],[('07',i) for i in number31],[('08',i) for i in number31],[('10',i) for i in number31],[('12',i) for i in number31]
Feb,Apr,Jun,Sep,Nov = [('02',i) for i in number30],[('04',i) for i in number30],[('06',i) for i in number30],[('09',i) for i in number30],[('11',i) for i in number30]

#for month in [May,Jun,Jul,Aug,Sep,Oct,Nov,Dec]:
#    for day in month:
#        dates.append('2017-%s-%s' % day)
#for day in Jan:
#    dates.append('2018-%s-%s' % day)
#for day in Feb[:21]:
#    dates.append('2018-%s-%s' % day)

dates = []
for i in number31[1:29]:    
    dates.append('2017-10-%s' % i )
      
        
# Define query function

def query(date,location_id):
    queryBody = {
     "date": date,  
     
     "location": {
       "locationType": "locationHierarchyLevel",
       "levelType": "discrete_visit_subzone",
       "id": location_id},
             
     "queryGranularity": {
       "type": "period",
       "period": "PT1H" },
             
    "filter": {
        "type": "bound",
        "dimension": "agent_year_of_birth",
        "lower": 1918},
            
    "dimensionFacets": [
            "agent_year_of_birth"],
    
     "aggregations":
     [
       {    
         "metric": "unique_agents",
         "type": "hyperUnique",
         "describedAs": "footfall"
       }
     ]
    }
    return queryBody

# Define output query results 
    
def result(dates, location_id):
    output = []
    for date in dates:        
        queryResponse = requests.post("https://apistore.datasparkanalytics.com:443/discretevisit/v2/query",
         data = json.dumps(query(date,location_id)),
         headers = {
           'Authorization': 'Bearer ' + token,
           'Content-Type': 'application/json'})
        output.append(queryResponse.json())
    return output


''' Part 3 Data Aggregation '''

def flat_dict(a):
    flat = {}
    flat.update({'timestamp':a['timestamp']})
    for i,v in a['event'].items():
        flat.update({i:v})
    return flat

locations = pd.read_csv(r'C:\Users\Lenovo 7000\OneDrive\Desktop\Dataspark\advertisement_locations.csv',delimiter = '\t')
df_list = []    
for i in range(len(locations.index)):
    name = locations['Subzone Name'][i]
    location_id = locations['Subzone Code'][i]
    results = result(dates,location_id)
    flat_output = []
    for l in results:
        for dictionary in l:
            flat_output.append(flat_dict(dictionary))
    df = pd.DataFrame(flat_output)   
    df_list.append(df)  

df_concated = pd.concat(df_list)

'''Part 4 Data cleaning'''
         
# Get age group
df_concated = df_concated[df_concated.agent_year_of_birth.str.contains("NA") == False]
df_concated['age'] = df_concated['agent_year_of_birth'].astype(int).apply(lambda x: 2018 - x)  
 
# define age group by dictionary d
d = {}
for x in range(0,13):
    d[x]='0-12'
for x in range(13,20):
    d[x]='13-19'
for x in range(20,30):
    d[x]='20-29'
for x in range(30,40):
    d[x]='30-39'
for x in range(40,50):
    d[x]='40-49'
for x in range(50,60):
    d[x]='50-59'
for x in range(60,101):
    d[x]='60-100'
df_concated['age_group'] = df_concated['age'].map(d)

# Extract datetime column
df_concated['datetime'] = df_concated.timestamp.apply(lambda x: x[:10]+' ' + x[11:19])

# Get the day in a week
d_week = {}
for x in ['02','09','16','23']:
    d_week[x] = 'Monday'
for x in ['03','10','17','24']:
    d_week[x] = 'Tuesday'
for x in ['04','11','18','25']:
    d_week[x] = 'Wednesday'
for x in ['05','12','19','26']:
    d_week[x] = 'Thursday'
for x in ['06','13','20','27']:
    d_week[x] = 'Friday'
for x in ['07','14','21','28']:
    d_week[x] = 'Saturday'
for x in ['08','15','22','29']:
    d_week[x] = 'Sunday'
df_concated['weekday'] = df_concated['datetime'].apply(lambda x: x[8:10]).map(d_week)
    
# Match the location name
subzone = pd.read_csv(r'C:\Users\Lenovo 7000\OneDrive\Desktop\Dataspark\subzone.csv',delimiter = '\t')
df_concated=df_concated.merge(subzone,how = 'left', left_on = 'discrete_visit_subzone', right_on = 'code')

df_concated.drop(['timestamp','agent_year_of_birth','code'],axis = 1,inplace = True)
df_concated.dropna(inplace = True)
df_concated.reset_index(inplace = True,drop = True)

df_concated.to_csv('age.csv')

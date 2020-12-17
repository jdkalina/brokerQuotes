import pandas as pd
import os
from fuzzywuzzy import fuzz
import numpy as np
from datetime import datetime,timedelta
from ftplib import FTP

os.chdir('/')

user = "r9000000"
passw = 'password'
uri = 'prodcddis.thomsonreuters.com'
dspFileName = 'Structured_Notes'

ftp = FTP(uri,user,passw)
ftp.cwd('Bulk_Reports')
today = datetime.today()
folder = f'{str(today.year)}{str(today.month).zfill(2)}{str(today.day).zfill(2)}'
ftp.cwd(folder)
files = ftp.nlst()
brokers = [file for file in files if dspFileName in file and 'note' not in file]
for file in brokers:
    with open('dspFiles/'+file,'wb') as f:
        ftp.retrbinary(f'RETR {file}', f.write)
ftp.quit()
files = os.listdir('dspFiles/')
data = pd.DataFrame()
for file in files:
    temp = pd.read_csv('dspFiles/'+file,compression='zip',sep='|',skiprows=1,dtype={'Quote_Perm_ID': str,'Maturity_Date':str,'Par_Value':str,'Effective_Date':str})
    data = data.append(temp.iloc[:,:-1])
c,n = 'Contributor_Name', 'Issuer_Name'
data.loc[data['Issuer_Name'] == 'GS FINANCE CORP','Issuer_Name'] = 'GOLDMAN'
data.reset_index(drop=True, inplace=True)
match = []
for i in range(data.shape[0]):
    string1 = data.loc[i,c]
    string2 = data.loc[i,n]
    if string1 is np.nan:
        match.append(0)
    else:
        value = fuzz.token_set_ratio(string1,string2)
        match.append(value)
data['match'] = match
data['pricing'] = (data[['Bid_Price','Mid_Price','Ask_Price']]>0).any(axis=1)
one_value = []
cusips = []
cins = []

for i in range(data.shape[0]):
    cusip = data.loc[i,'CUSIP']
    cin = data.loc[i,'CIN_Code']
    ric = data.loc[i,'RIC'].split('=')[0]
    if cusip is not np.nan:
        cusips.append(cusip)
        cins.append('')
        one_value.append(cusip)
    elif cin is not np.nan:
        cusips.append('')
        cins.append(cin)
        one_value.append(cin)
    elif len(ric) == 12:
        value = ric[2:11]
        if value[0].isalpha():
            cusips.append('')
            cins.append(value)
            one_value.append(value)
        else:
            cusips.append(value)
            cins.append('')
            one_value.append(value)
    elif len(ric) == 9:
        if ric[0].isalpha():
            cusips.append('')
            cins.append(ric)
            one_value.append(ric)
        else:
            cusips.append(ric)
            cins.append('')
            one_value.append(ric)
    else:
        cusips.append('')
        cins.append('')
        one_value.append('')

one_price = []
for i in range(data.shape[0]):
    bid = data.loc[i,'Bid_Price']
    mid = data.loc[i,'Mid_Price']
    ask = data.loc[i,'Ask_Price']
    if bid > 0:
        one_price.append(bid)
    elif mid > 0:
        one_price.append(mid)
    elif ask > 0:
        one_price.append(ask)
    else:
        one_price.append(np.nan)
data['one_price'] = one_price
data['new_cusip'] = cusips
data['new_cin'] = cins
data['one_id'] = one_value

data['is_today'] = pd.to_datetime(data['Effective_Date']) == pd.to_datetime(datetime.strftime(today,"%Y-%m-%d"))
data = data[data['is_today']]
data = data.sort_values(['pricing','match'],ascending=False)
data = data[['new_cusip','CIN_Code','one_price','Trade_Date','Mid_Price']].copy()
data.columns = ['CUSIP','CIN_Code','Bid_Price','Trade_Date','Mid_Price']
data.fillna('',inplace=True)
metadata = f'Structured_Notes|FPC|4E1F|{datetime.strftime(datetime.today(),format="%Y%m%d")}|ALL|ALL\n'
cusip,cin,bid,date,mid = data.columns
beta = []
beta.append(metadata)
beta.append(f'{cusip}|{cin}|{bid}|{date}|{mid}\n')
for i in range(data.shape[0]):
    cusip,cin,bid,date,mid = data.iloc[i,:]
    beta.append(f'{cusip}|{cin}|{bid}|{date}|{mid}\n')

with open(f'SN{folder}.txt','w') as f:
    f.writelines(beta)

for file in files:
    os.remove('dspFiles/'+file)
user = "r9000000"
passw = 'password'
uri = 'hosted.datascope.reuters.com'
ftp = FTP(uri,user,passw)
ftp.cwd('reports/pricing_service')
with open(f'SN{folder}.txt','rb') as f:
    ftp.storbinary(f'STOR {f"SN{folder}.txt"}', fp = f)
ftp.quit()

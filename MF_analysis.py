import pandas as pd
import urllib, json #data input from url and reading json
from pandas.io.json import json_normalize
from dateutil.relativedelta import relativedelta #to calculate date x year earlier than now
import dateutil.parser # 'Data time' format to 'Date'
from nsepy import get_history #https://nsepy.readthedocs.io/en/latest/
from nsepy.history import get_price_list #https://nsepy.readthedocs.io/en/latest/
import quandl #https://docs.quandl.com/docs/python-installation
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt #used for graph
import seaborn as sns

sns.set()

def sr_calc (input_df):
    output_df = pd.DataFrame()
    sample_duration = 3
    annual_std_factor = 12 #12-Monthly return, 252-Daily return
    input_df = input_df.sort_values(by='date') #sort joined table by date from first to last date - affects daily return calculation otherwise
    sample_data_sr = input_df['date'] > pd.Timestamp(date.today() - relativedelta(years=sample_duration)).normalize()
    input_df = input_df.loc[sample_data_sr]
    
    distinct_MF_index_name = input_df['MF_index_name'].unique() 
    
    for items in distinct_MF_index_name:
        each_set = input_df.loc[input_df.MF_index_name==items]
        
        #for monthly return
        if annual_std_factor == 12: #condition to fetch monthend records only
            each_set.set_index('date', inplace = True) 
            each_set = each_set.groupby(each_set.index.strftime('%Y-%m')).tail(1)
            each_set = each_set.reset_index()
        
        each_set['daily_return_mf'] = each_set['Close'].pct_change(1)
        each_set_first_row = each_set.loc[each_set.date == each_set.date.min(),['Close']]
        each_set_last_row = each_set.loc[each_set.date == each_set.date.max(),['Close']]
        mf_annual_return = ((each_set_last_row['Close'].mean()-each_set_first_row['Close'].mean())/each_set_first_row['Close'].mean())/sample_duration
        
        std_of_return = each_set['daily_return_mf'].std()
        std_annual = std_of_return * ((annual_std_factor)**0.5)
        sr = (mf_annual_return - 0.05)/std_annual
        
        output_df = output_df.append({'MF_Scheme':items,
                                      'MF annual return':mf_annual_return,
                                      'STD of daily return':std_of_return,
                                      'Annualise STD':std_annual,
                                      'Sharp Ratio':sr,
                                      },ignore_index=True)
    return (output_df)

consolidated_nav = pd.DataFrame()
consolidated_index = pd.DataFrame()
sr_output = pd.DataFrame()
MF_Spreadsheet = pd.read_excel('MF_analysis.xlsx', sheet_name=None)
MF_scheme_input = MF_Spreadsheet['MF_Static']
if (dateutil.parser.parse(str(MF_Spreadsheet['Consolidated_NAV'].max()['date'])).date()) <= (date.today() - timedelta(10)):
    for items, source, scheme in zip(MF_scheme_input['reference'], MF_scheme_input['source'], MF_scheme_input['MF_Scheme']):
        if source == 'mfapi':
            # Stage 1: get MF scheme life time nav from mfapi
            response = urllib.request.urlopen('https://api.mfapi.in/mf/'+ str(items))
            summary = json.loads(response.read())
            data = json_normalize(summary['data'])
            meta = json_normalize(summary['meta'])
            no_of_dates = len(summary['data']) #no of elements in a jason
            count = 0
            single_mf_data = pd.DataFrame()
            while count < no_of_dates:
                single_mf_data = single_mf_data.append({'MF_index_name':scheme,
                                                        'date':data['date'][count],
                                                        'Close':data['nav'][count]
                                                        },ignore_index=True)
                count = count + 1
            single_mf_data['date']=pd.to_datetime(single_mf_data['date'],dayfirst=True) #convert string to Date format with dayfirst=tru: 12-06-2019 --> 2019-06-12 ie 12 is considered a Day and not Month
            single_mf_data['Close']=pd.to_numeric(single_mf_data['Close']) #convert string to numeric format              
                       
            consolidated_nav = single_mf_data.append(consolidated_nav, ignore_index=True) #https://pythonprogramming.net/concatenate-append-data-analysis-python-pandas-tutorial/

        if source == 'nsepy':
            # Stage 2: get index details from nsepy
            each_nse_index = get_history(symbol=items,
                               start=datetime(2013,1,1),
                               end=datetime.today(),
                               index=True)
            each_nse_index['MF_index_name'] = scheme
            each_nse_index = each_nse_index.reset_index() #to remove existing index 'Date' to a default index
            each_nse_index = each_nse_index.rename(columns={'Date':'date'})
            each_nse_index['date']=pd.to_datetime(each_nse_index['date']) #convert string to Date format

            consolidated_index = each_nse_index.append(consolidated_index, ignore_index=False)
        
        if source =='Quandl':
            # Stage 3: get index details from quandl for BSE indices
            quandl.ApiConfig.api_key = '6s_yHyw-PaVWERvNWvRR'
            bse_index_key = 'BSE/'+str(items)
            each_bse_index = quandl.get(bse_index_key, start_date='2013-01-01', end_date=datetime.today())
            each_bse_index['MF_index_name'] = scheme
            each_bse_index = each_bse_index.reset_index() #to remove existing index 'Date' to a default index
            each_bse_index = each_bse_index.rename(columns={'Date':'date'})
            each_bse_index['date']=pd.to_datetime(each_bse_index['date']) #convert string to Date format
            
            consolidated_index = each_bse_index.append(consolidated_index, ignore_index=False)
else:
    consolidated_nav = MF_Spreadsheet['Consolidated_NAV']
    consolidated_index = MF_Spreadsheet['Consolidated_index']

#consolidate MF & Index information
result = pd.concat([consolidated_index,consolidated_nav],sort=False)

#Stage 2: Sharp Ration calculation
sr_output = sr_calc(result)
sr_output = pd.merge(MF_scheme_input, sr_output,how ='outer', on='MF_Scheme')

#Stage 3: populate graph - combined line graph for MF & corresponding index

result_graph = pd.merge(result, MF_scheme_input, how='left',
                        left_on='MF_index_name', right_on='MF_Scheme')

#value_to_check = pd.Timestamp(date.today().year, 1, 1) #'2019-05-01 00:00:00'
value_to_check = pd.Timestamp(date.today() - relativedelta(years=2))

filter_nav = result_graph['date'] > value_to_check
filtered_df = result_graph[filter_nav]

distinct_graph = filtered_df['ref_index'].unique() 
for graph in distinct_graph: 
    filtered_df_graph = filtered_df.loc[filtered_df['ref_index'].isin([graph])]      
    distinct_MF_filtered_df = filtered_df_graph['MF_index_name'].unique() 
    
    Normalise_close_graph_consolidated = pd.DataFrame()     
    for items in distinct_MF_filtered_df:
        Normalise_close_graph = filtered_df_graph.loc[filtered_df_graph.MF_index_name==items]
        Normalise_close_graph = Normalise_close_graph.sort_values(by='date', ascending=True)
        Normalise_close_graph['Normalise_close'] = 100 * (1 - Normalise_close_graph.iloc[0].Close / Normalise_close_graph['Close'])
        #Normalise_close_graph['Close'] = Normalise_close_graph['Close'].apply(lambda x: x / x[0])
        
        Normalise_close_graph_consolidated = Normalise_close_graph_consolidated.append(Normalise_close_graph,ignore_index=False)    
    g, ax = plt.subplots(figsize = (20,6))
    g = sns.relplot(x='date',y='Normalise_close', 
                data=Normalise_close_graph_consolidated, 
                kind='line', 
                hue='MF_index_name',
                size = 'mf_or_index', 
                sizes=(3, 1), 
                col='ref_index',col_wrap=2, 
                facet_kws=dict(sharey=False, sharex=False),
                ax=ax)
    x_dates = Normalise_close_graph_consolidated['date'].dt.strftime('%Y-%m').sort_values().unique()
    ax.set_xticklabels(labels=x_dates, rotation=45)
    #g.savefig("myfig.png")

# Stage 4: Writing output into original Spreadsheet
MF_Spreadsheet['MF_Static'] = MF_scheme_input
MF_Spreadsheet['Consolidated_NAV'] =consolidated_nav
MF_Spreadsheet['Consolidated_index'] =consolidated_index
MF_Spreadsheet['SR_Dashboard'] =sr_output
writer_orig = pd.ExcelWriter('MF_analysis.xlsx', engine='xlsxwriter',datetime_format='dd-mmm-yyyy',date_format='dd-mmm-yyyy') #engine='openpyxl' - for csv file, mode='a'
for ws_name, df_sheet in MF_Spreadsheet.items(): # ws_name = tab name & df_sheet = tab containt
    df_sheet.to_excel(writer_orig, index = False, sheet_name=ws_name)
    workbook = writer_orig.book
    worksheet = writer_orig.sheets[ws_name]
    if ws_name == 'SR_Dashboard':
        num_format = workbook.add_format({'align': 'right', 'num_format': '#,##0.00'})
        percent_format = workbook.add_format({'num_format': '0.00%'}) 
        worksheet.set_column('I:I', None, num_format)
        worksheet.set_column('F:H', None, percent_format) 
    for i, col in enumerate(df_sheet.columns): #excel autoadjust col width - https://stackoverflow.com/questions/17326973/is-there-a-way-to-auto-adjust-excel-column-widths-with-pandas-excelwriter
        column_len = df_sheet[col].astype(str).str.len().max()
        column_len = max(column_len, len(col)) + 2
        worksheet.set_column(i, i, column_len)
    worksheet.freeze_panes(1,0)
writer_orig.save()

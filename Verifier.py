import pandas as pd
import openpyxl as pyxl
import numpy as np
import glob
import datetime

def readFiles():
    path = r'E:\data\data_verification\20200201'
    print('Reading input files in the path ' + path + ' ...')
    all_files = glob.glob(path + "/*.csv")

    print('Found these files in the path ', all_files)

    dfListRawFiles = []

    for filename in all_files:
        print('Reading ', filename , ' ...')
        df = pd.read_csv(filename, low_memory=False, skiprows=1, thousands=r',',index_col=None, header=0)
        dfListRawFiles.append(df)

    print('All Read, now Concatenating them...')
    df_big = pd.concat(dfListRawFiles, axis=0, ignore_index=True)
    print('Concatenated successfully...')

    return df_big

# verification functions ###############################################################
def check_count(df:pd.DataFrame):
    df_group_by = df.pivot_table(values='SITE', index='Time', aggfunc=pd.Series.nunique)
    df_count_check = df_group_by[df_group_by['SITE'] <= 10900]

    return df_count_check

def check_thrput(df:pd.DataFrame, kpi_dict):
    throughput_kpi = kpi_dict['thrput']
    payload_kpi = kpi_dict['payload']
    avail_kpi = kpi_dict['avail']

    df_check_thrput = df[((df[throughput_kpi].isna())
                             | (df[throughput_kpi] <= 0))
                            &
                            ((df[payload_kpi] > 0)
                             #commented as avail doesn't correlate directly to payload or throughput
                             #| (pd.to_numeric(df[avail_kpi], errors='coerce') > 0)
                             )
                            ]
    df_check_thrput = df_check_thrput.reset_index(drop=True)

    return df_check_thrput

def check_payload(df:pd.DataFrame, kpi_dict):
    throughput_kpi = kpi_dict['thrput']
    payload_kpi = kpi_dict['payload']
    avail_kpi = kpi_dict['avail']

    throughputMBps = round(df[throughput_kpi]/8/1024,2)
    df_check_payload = df[((df[payload_kpi].isna())
                              | (df[payload_kpi] <= 0))
                             &
                             ((throughputMBps > 0)
                             #commented as avail doesn't correlate directly to payload or throughput
                             # | (pd.to_numeric(df[avail_kpi], errors='coerce') > 0)
                              )
                             ]
    df_check_payload = df_check_payload.reset_index(drop=True)
    return df_check_payload

def check_avail(df:pd.DataFrame, kpi_dict):
    throughput_kpi = kpi_dict['thrput']
    payload_kpi = kpi_dict['payload']
    avail_kpi = kpi_dict['avail']

    df_check_avail = df[((df[avail_kpi].isna())
                              | (pd.to_numeric(df[avail_kpi], errors='coerce') <= 0))
                             &
                             ((df[throughput_kpi] > 0)
                             | (df[payload_kpi] > 0))
                             ]

    df_check_avail = df_check_avail.reset_index(drop=True)
    return df_check_avail

def check_all_KPIs(df:pd.DataFrame, kpi_dict):
    throughput_kpi = kpi_dict['thrput']
    payload_kpi = kpi_dict['payload']
    avail_kpi = kpi_dict['avail']

    df_check_all_KPIs = df[((df[avail_kpi].isna())
                              | (pd.to_numeric(df[avail_kpi], errors='coerce') <= 0))
                             &
                              ((df[throughput_kpi].isna())
                               | (df[throughput_kpi] <= 0))
                             &
                              ((df[payload_kpi].isna())
                              | (df[payload_kpi] <= 0))
                             ]

    df_check_all_KPIs = df_check_all_KPIs.reset_index(drop=True)
    return df_check_all_KPIs


def summarize_all_KPIs_count(df:pd.DataFrame, kpi_dict):
    df_summarize_all_KPIs_count = df_check_all_KPIs.pivot_table(values='Time', index='SITE', aggfunc=pd.Series.count, )
    # df_summarize_all_KPIs_count.sort_values(by='Time', ascending=False)
    df_summarize_all_KPIs_count = df_summarize_all_KPIs_count.reset_index()


    return df_summarize_all_KPIs_count

def excludeNotSOACsites( df_raw_input: pd.DataFrame
                        ,df_morning: pd.DataFrame
                        ,i):

    if len(df_raw_input.index) > 0:
        # print("Adding SiteID column with this format TXXXX ...")
        df_raw_input['SiteID'] = df_raw_input['SITE'].str.extract(r'([A-Z]\d{4})') #to unify the site id format to join

        #print("Merging data frames for SOAC check of ", i , "G ...")
        df_merged = pd.merge(df_raw_input, df_morning, on='SiteID', how='inner')

        if i == 2: #2G
            df_merged = df_merged[df_merged['2G SOAC Date'].notnull()]
        elif i == 3: #3G
            df_merged = df_merged[(df_merged['3G 2100 SOAC Date'].notnull()) | (df_merged['3G 900 SOAC Date'].notnull())]
        elif i == 4: #4G
            df_merged = df_merged[(df_merged['LTE 1800 SOAC Date'].notnull()) | (df_merged['LTE 2600 SOAC Date'].notnull())
                                  | (df_merged['LTE 900 SOAC Date'].notnull())]
        return df_merged
    else:
        return df_raw_input #just return it without any modification

def excludeDeactivatedSites(df_raw_input: pd.DataFrame
                            ,df_deactivateList: pd.DataFrame
                            ,i):

    if len(df_raw_input.index) > 0:
        # print("Adding SiteID column with this format TXXXX ...")
        df_raw_input['SiteID'] = df_raw_input['SITE'].str.extract(r'([A-Z]\d{4})') #to unify the site id format to join
        df_deactivateList['SiteID'] = df_deactivateList['MOENTITYNAME'].str.extract(r'([A-Z]\d{4})') #to unify the site id format to join

        df_merged = pd.merge(df_raw_input, df_deactivateList, on='SiteID', how='left')

        df_deactived_sites = df_merged[df_merged['MOENTITYNAME'].notnull()]
        if len(df_deactived_sites.index) > 0:
            print('Excluding ', len(df_deactived_sites.index), ' number of deactivated sites.')
            print('samples:')
            print(df_deactived_sites)
            print('before deactivate: ', len(df_merged.index))

        df_merged = df_merged[df_merged['MOENTITYNAME'].isnull()] # where site is not in deactivated list

        df_merged.reset_index()

        if len(df_deactived_sites.index) > 0:
            print('After deactivate: ', len(df_merged.index))

        return df_merged
    else:
        return df_raw_input #just return it without any modification

def excludeUnsyncSites( df_raw_input: pd.DataFrame
                        ,df_unsync: pd.DataFrame
                        ,i):

    if len(df_raw_input.index) > 0:
        # print("Adding SiteID column with this format TXXXX ...")
        regexPattern = r'([A-Z]\d{4})'
        df_raw_input['SiteID'] = df_raw_input['SITE'].str.extract(regexPattern) #to unify the site id format to join
        df_unsync = df_unsync.melt(id_vars=['site_id'], var_name='DateHour', value_name='unsync_flag')
        df_unsync['SiteID'] = df_unsync['site_id'].str.extract(regexPattern) #to unify the site id format to join

        #reformatting time column to match with raw data.
        df_unsync['Time'] = df_unsync['DateHour'].str[:4] + '-' + df_unsync['DateHour'].str[4:6] + '-' + df_unsync['DateHour'].str[6:8] \
                            + ' ' + df_unsync['DateHour'].str[8:10] + ':00:00'

        #print('Total number of raw data rows:', len(df_raw_input.index))
        #print("Merging data frames for SOAC check of ", i , "G ...")

        print(df_raw_input.columns)
        print(df_unsync.columns)
        df_merged = pd.merge(df_raw_input, df_unsync, on=['SiteID','Time'], how='left')

        #simply remove the ones with unsync flag = 1
        df_merged = df_merged[df_merged['unsync_flag'] != 1]
        df_merged.reset_index()

        return df_merged
    else:
        return df_raw_input #just return it without any modification


def readMorningReport():
    print("Reading morning report...")
    df_morning = pd.read_excel(r'E:\data\data_verification\20200201\Irancell RAN Morning Report 31-Jan-2020.xlsx'
                           , sheet_name='On-Air Sites', skiprows=1)

    df_morning = df_morning[['SiteID', '2G SOAC Date', '3G 2100 SOAC Date', '3G 900 SOAC Date', 'LTE 1800 SOAC Date'
        ,'LTE 2600 SOAC Date', 'LTE 900 SOAC Date']]

    return df_morning

def readDeactivatedSitesReport():
    print("Reading Deactivated list report...")
    df_deactivatedSites = pd.read_excel(r'E:\data\data_verification\20200201\MAPS_active_deactive_lilst.xls'
                           , sheet_name='title_1')

    return df_deactivatedSites

def readUnsyncList():
    print("Reading Unsync list ...")
    df_UnsyncList = pd.read_excel(r'E:\data\data_verification\20200201\Ericsson_unsync_2020_02_01_0828.xlsx'
                           , sheet_name='unsync_list')

    df_UnsyncList = df_UnsyncList.drop(['DAILY_SUMMATION'],axis=1)

    return df_UnsyncList

def read4GThrputCounters():
    print("Reading 4G Throughput files ...")
    filename = r'E:\data\data_verification\20191227\thrput counters\ericsson_4g_thrput_counters_2019122911_eae6362b63244251bbc6d161ada2e2f0.csv'
    df_4gthrput_ericsson = pd.read_csv(filename, low_memory=False, skiprows=1, thousands=r',', index_col=None, header=0)
    df_4gthrput_ericsson.rename(columns={"Ericsson_ENodeB": "ENODEB"}, inplace=True)
    print(df_4gthrput_ericsson.columns)

    filename = r'E:\data\data_verification\20191227\thrput counters\huawei_4g_thrput_counters_2019122911_ff9bb89a509044d0997ddee5aced7b80.csv'
    df_4gthrput_huawei = pd.read_csv(filename, low_memory=False, skiprows=1, thousands=r',', index_col=None, header=0)
    df_4gthrput_huawei.rename(columns={"Huawei_LTE_eNodeB": "ENODEB"}, inplace=True)
    print(df_4gthrput_huawei.columns)

    filename = r'E:\data\data_verification\20191227\thrput counters\nokia_4g_thrput_counters_2019122911_af2d9f49651d4e54bc6d79075f00a4d6.csv'
    df_4gthrput_nokia = pd.read_csv(filename, low_memory=False, skiprows=1, thousands=r',', index_col=None, header=0)
    df_4gthrput_nokia.rename(columns={"NSN_FDD_LNBTS": "ENODEB"}, inplace=True)
    print(df_4gthrput_nokia.columns)

    df_merged_1 = pd.merge(df_4gthrput_ericsson, df_4gthrput_huawei, on=['ENODEB','Time'], how='outer')

    df_merged_2 = pd.merge(df_merged_1, df_4gthrput_nokia, on=['ENODEB','Time'], how='outer')

    print(df_merged_2.columns)

    regexPattern = r'([A-Z]{1}\d{4}[A-Z])'
    df_merged_2['SiteID'] = df_merged_2['ENODEB'].str.extract(regexPattern)  # to unify the site format to join

    return df_merged_2


def read2GThrputCounters():
    print("Reading 2G Throughput files ...")
    filename = r'E:\data\data_verification\20191227\thrput counters\ericsson_2g_thrput_counters_2019122911_c8e1db436bfa4c059bb15c4da2aeeee3.csv'
    df_2gthrput_ericsson = pd.read_csv(filename, low_memory=False, skiprows=1, thousands=r',', index_col=None, header=0)
    df_2gthrput_ericsson.rename(columns={"Ericsson_BTS": "BTS"}, inplace=True)
    print(df_2gthrput_ericsson.columns)

    filename = r'E:\data\data_verification\20191227\thrput counters\huawei_2g_thrput_counters_2019122911_a3aa33a1688a4253ba714be36d194784.csv'
    df_2gthrput_huawei = pd.read_csv(filename, low_memory=False, skiprows=1, thousands=r',', index_col=None, header=0)
    df_2gthrput_huawei.rename(columns={"Huawei_BTS": "BTS"}, inplace=True)
    print(df_2gthrput_huawei.columns)

    filename = r'E:\data\data_verification\20191227\thrput counters\nokia_2g_thrput_counters_2019122911_720b161101734ef5af8929f10ced1952.csv'
    df_2gthrput_nokia = pd.read_csv(filename, low_memory=False, skiprows=1, thousands=r',', index_col=None, header=0)
    df_2gthrput_nokia.rename(columns={"NSN_BTS": "BTS"}, inplace=True)
    print(df_2gthrput_nokia.columns)

    df_merged_1 = pd.merge(df_2gthrput_ericsson, df_2gthrput_huawei, on=['BTS','Time'], how='outer')

    df_merged_2 = pd.merge(df_merged_1, df_2gthrput_nokia, on=['BTS','Time'], how='outer')

    print(df_merged_2.columns)

    regexPattern = r'([A-Z]{1}\d{4})'
    df_merged_2['SiteID'] = df_merged_2['BTS'].str.extract(regexPattern)  # to unify the site format to join

    return df_merged_2


def include4GThroughputCountres(df_check_thrput, df_4g_thrput):
    print("Including 4g throughput counters...")
    df_merged = pd.merge(df_check_thrput, df_4g_thrput, on=['SiteID', 'Time'], how='left')

    return df_merged

def include2GThroughputCountres(df_check_thrput, df_2g_thrput):
    print("Including 2g throughput counters...")
    df_merged = pd.merge(df_check_thrput, df_2g_thrput, on=['SiteID', 'Time'], how='left')

    return df_merged


#2G first
kpi_dict = { '2g_kpis': {
                        'thrput': '2G_EGPRS_LLC_THROUGHPUT_IR(Kbps)'
                        ,'payload' : '2G_PAYLOAD_LLC_TOTAL_KBYTE_IR(KB)'
                        ,'avail': '2G_TCH_AVAILABILITY_IR(%)'
                },
             '3g_kpis': {
                        'thrput': '3G_Throughput_HS_DC_NodeB_kbps_IR(%)'
                        ,'payload': '3G_PAYLOAD_TOTAL_3G_KBYTE_IR(KB)'
                        ,'avail': '3G Cell_Avail_Sys_IR(%)'
                },
             '4g_kpis': {
                        'thrput': '4G_Throughput_UE_DL_kbps_IR(Kbps)'
                        ,'payload': '4G_PAYLOAD_TOTAL_KBYTE_IR(KB)' #'4G_PAYLOAD_TOTAL_MBYTE_IR(MB)'
                        ,'avail': '4G_CELL_AVAIL_SYS_IR'
                }
        }

#start here

print('Started at: ', datetime.datetime.now())

writer = pd.ExcelWriter(r'E:\data\data_verification\20200201\verification_report_20200201.xlsx')

# df_4g_thrput = read4GThrputCounters()
# df_2g_thrput = read2GThrputCounters()
df = readFiles()
df_morning = readMorningReport()
df_deactivatedList = readDeactivatedSitesReport()
df_unsyncList = readUnsyncList()

print('Now verifying data...')

total_rows = len(df.index)

arr_result_summary_data = []

print('Total rows: ', total_rows)
arr_result_summary_data.append(['Total Rows',total_rows])

df_result_summary = pd.DataFrame()
df_result_summary.to_excel(writer, sheet_name='Test Result Summary')

# 1) site count issues
df_count_check = check_count(df)
rowsWithCountIssue = len(df_count_check.index)
print('Rows with count issue: ', rowsWithCountIssue)
df_count_check.to_excel(writer, sheet_name='Count check - less than 10900')
arr_result_summary_data.append(['Rows with count issue',rowsWithCountIssue])

# 2) per KPI and per technology
for i in range(2,5):
    print('Verifying ' +  str(i) + 'G KPIs...' )
    tech_label = str(i) + 'g_kpis'
    tech_kpi_dict = kpi_dict[tech_label]

    df_check_thrput = check_thrput(df, tech_kpi_dict)
    df_check_payload = check_payload(df, tech_kpi_dict)
    df_check_avail = check_avail(df, tech_kpi_dict)
    df_check_all_KPIs = check_all_KPIs(df, tech_kpi_dict)
    df_summarize_all_KPIs_count = summarize_all_KPIs_count(df_check_all_KPIs, tech_kpi_dict)

    print("Excluding Not SOAC sites for " + str(i) + "G ...")
    df_check_thrput = excludeNotSOACsites(df_check_thrput, df_morning,i)
    df_check_payload = excludeNotSOACsites(df_check_payload, df_morning,i)
    df_check_avail = excludeNotSOACsites(df_check_avail, df_morning,i)
    df_check_all_KPIs = excludeNotSOACsites(df_check_all_KPIs, df_morning,i)
    df_summarize_all_KPIs_count = excludeNotSOACsites(df_summarize_all_KPIs_count, df_morning,i)

    if i == 2:
        # deactived sites are only available for 2G at the moment
        print("Excluding deactivated sites...")
        df_check_thrput = excludeDeactivatedSites(df_check_thrput, df_deactivatedList, i)
        df_check_payload = excludeDeactivatedSites(df_check_payload, df_deactivatedList, i)
        df_check_avail = excludeDeactivatedSites(df_check_avail, df_deactivatedList, i)
        df_check_all_KPIs = excludeDeactivatedSites(df_check_all_KPIs, df_deactivatedList, i)
        df_summarize_all_KPIs_count = excludeDeactivatedSites(df_summarize_all_KPIs_count, df_deactivatedList, i)

        # adding 2g throughput counters for ease the investigation of the issues
        # df_check_thrput = include2GThroughputCountres(df_check_thrput, df_2g_thrput)

    print("Excluding Unsync sites for " + str(i) + "G ...")
    df_check_thrput = excludeUnsyncSites(df_check_thrput, df_unsyncList,i)
    df_check_payload = excludeUnsyncSites(df_check_payload, df_unsyncList,i)
    df_check_avail = excludeUnsyncSites(df_check_avail, df_unsyncList,i)
    df_check_all_KPIs = excludeUnsyncSites(df_check_all_KPIs, df_unsyncList,i)

    # if i == 4: # adding 4g throughput counters for ease the investigation of the issues
    #     df_check_thrput = include4GThroughputCountres(df_check_thrput, df_4g_thrput)

    ##Because summarized sheet doesn't have a datetime column, it cannot be sent to unsync exclusion method
    ##df_summarize_all_KPIs_count = excludeUnsyncSites(df_summarize_all_KPIs_count, df_unsyncList,i)

    df_check_thrput.reset_index(inplace=True, drop=True)
    df_check_payload.reset_index(inplace=True, drop=True)
    df_check_avail.reset_index(inplace=True, drop=True)
    df_check_all_KPIs.reset_index(inplace=True, drop=True)
    df_summarize_all_KPIs_count.reset_index(inplace=True, drop=True)

    rowsWithThroughputIssue = len(df_check_thrput.index)
    rowsWithPayloadIssue = len(df_check_payload.index)
    rowsWithAvailIssue = len(df_check_avail.index)
    rowsWithAllKPIsIssue = len(df_check_all_KPIs.index)
    rowsWithSummarizedAllKPIsIssue = len(df_summarize_all_KPIs_count.index)

    testThrput = 'Rows with ' + str(i) + 'g throughput issue: '
    testPayload = 'Rows with ' + str(i) + 'g payload issue: '
    testAvail = 'Rows with ' + str(i) + 'g availability issue: '
    testAllKpis = 'Rows with all ' + str(i) + 'g KPIs issue: '
    testSummarizedAllKpis = 'Sites with all ' + str(i) + 'g KPIs issue: '

    print(testThrput , rowsWithThroughputIssue)
    print(testPayload, rowsWithPayloadIssue)
    print(testAvail, rowsWithAvailIssue)
    print(testAllKpis, rowsWithAllKPIsIssue)
    print(testSummarizedAllKpis, rowsWithSummarizedAllKPIsIssue)

    arr_result_summary_data.append([testThrput, rowsWithThroughputIssue])
    arr_result_summary_data.append([testPayload, rowsWithPayloadIssue])
    arr_result_summary_data.append([testAvail, rowsWithAvailIssue])
    arr_result_summary_data.append([testAllKpis, rowsWithAllKPIsIssue])
    arr_result_summary_data.append([testSummarizedAllKpis, rowsWithSummarizedAllKPIsIssue])

    print('Exporting into excel report for ' + tech_label + '...')
    df_check_thrput.to_excel(writer, sheet_name='Throughput ' + str(i) + 'g issues')
    df_check_payload.to_excel(writer, sheet_name='Payload ' + str(i) + 'g issues')
    df_check_avail.to_excel(writer, sheet_name='Availability ' + str(i) + 'g issues')

#    if i != 4: #because for 4G, we have so many sites which don't have support for 4G at all. so no need to report them as issue. morever, they put heavy burden on the program and the output file.
    df_check_all_KPIs.to_excel(writer, sheet_name='All ' + str(i) + 'g KPIs issues')
    df_summarize_all_KPIs_count.to_excel(writer, sheet_name='Summarized ' + str(i) + 'g Sites with issues')

df_result_summary = pd.DataFrame(data=arr_result_summary_data, columns=['Test', 'Result'])
df_result_summary.to_excel(writer, sheet_name='Test Result Summary')

writer.save()

print('All Completed.')
print('Completed at: ', datetime.datetime.now())

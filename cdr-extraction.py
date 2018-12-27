from IPython import get_ipython
import os
import pandas as pd
import numpy as np
import time
import concurrent.futures
start_time = time.time()


list_date = [180101, 180102, 180103, 180104, 180105, 180106, 180107, 180108, 180109, 180110, 180111, 180112, 180113, 180114, 180115, 180116, 180117, 180118, 180119, 180120, 180121, 180122, 180123, 180124, 180125, 180126, 180127, 180128, 180129, 180130, 180131, 180201, 180202, 180203, 180204, 180205, 180206, 180207, 180208, 180209, 180210, 180211, 180212, 180213, 180214, 180215, 180216, 180217, 180218, 180219, 180220, 180221, 180222, 180223, 180224, 180225, 180306, 180307, 180308, 180309, 180310, 180311, 180312, 180313, 180314, 180315, 180316, 180317, 180318, 180319, 180320, 180321, 1803221, 180322, 180323, 180324, 180325, 180326, 180327, 180328, 180329, 180330, 180331, 180401, 180402, 180403, 180403, 180404, 180405, 180406, 180407, 180408, 180409, 180410, 180411, 180412, 180413, 180414, 180415, 180416, 180417, 180418, 180419, 180420, 180421, 180422, 180423, 180425, 180426, 180427, 180428, 180429, 180430, 180501, 180502, 180503, 180504, 180505, 180506, 180507, 180508, 180509, 180510, 180511, 180512, 180513, 180514, 180515, 180516, 180517, 180518, 180519, 180520, 180521, 180522, 180523, 180524, 180525, 180526, 180527, 180528, 180529, 180530, 180601, 180602, 180603, 180604, 180605, 180607, 180608, 180609, 180610, 180611, 180612, 180613, 180614, 180615, 180616, 180617, 180619, 180620, 180621, 180622, 180623, 180624, 180625, 180626, 180627, 180628, 180629]


def the_function(codeDate):
    dflist = []
    theend=0
    del dflist
    del theend
    dflist = []
    totalfile = 0
    for filename in os.listdir('../../../msc/'):
        if (filename.startswith("MSCPNGL2TTFILE."+ str(codeDate)) or
            filename.startswith("MSCPNGL1TTFILE."+ str(codeDate)) or
            filename.startswith("POM4TTFILE."+ str(codeDate)) or
            filename.startswith("POM2TTFILE."+ str(codeDate))) :
            totalfile +=1
            datacsv = pd.read_csv('../../../msc/'+filename, names = header_table, low_memory=False)
            df = pd.DataFrame(datacsv,columns=[3,19,20,21,22,115,116,119,121])
            processthecsv = df.loc[(df[3] == 'SMSOriginating') | (df[3] == 'SMSTerminating') | (df[3] == 'MSOriginating') | (df[3] == 'MSTerminating')]
            print(totalfile)
            dflist.append(processthecsv)
    print("--- %s seconds for loop---" % (time.time() - start_time))
    theend = pd.concat(dflist,ignore_index=True,sort=False)
    print("--- %s seconds after concat---" % (time.time() - start_time))
    theend.to_csv('../../../msc/generated_data/'+str(codeDate)+'_selected_columns_concat.csv',index=False)
    print("--- %s seconds after write---" % (time.time() - start_time))
    print('Done')

with concurrent.futures.ProcessPoolExecutor() as executor:
    for f, zipped_file in zip(list_date, executor.map(the_function, list_date)):
        print('run')





import csv, codecs, hashlib
import os
from datetime import datetime, timedelta, time, date
from os import listdir, makedirs
from os.path import isfile, join, exists
import concurrent.futures

root_folder     = './'
header          = ['interaction', 'direction', 'antenna_id', 'call_duration', 'correspondent_id' 'datetime']
encoding        = 'utf-8'

start_date = date(2018, 2, 26)
end_date = date(2018, 3, 5)

listofdate = [  
				[date(2018, 7, 2),date(2018, 7, 9),'user-data-week-27/'],
                [date(2018, 7, 9),date(2018, 7, 16),'user-data-week-28/']
             ]

def runprocess(thedate):
    start_date = thedate[0]
    end_date = thedate[1]
    dest_folder = thedate[2]
    list_dates = []
    for n in range(int ((end_date - start_date).days)):
            n_dates = start_date + timedelta(n)
            the_day = n_dates.strftime('%y%m%d')
            print the_day
            list_dates.append(str(the_day)+'_selected_columns_concat.csv')

    def iterate(filename):
            with codecs.open(root_folder + filename, "r", encoding) as steam_in:
                print "start " + filename + "!"

                reader  = csv.reader(steam_in)
                reader.next()

                user_dict = {}
                for line in reader:
                    try:
                        line[6] = int(float(line[6]))
                    except:
                        line[6]


                    if (line[0] in ["SMSOriginating","MSOriginating"]):

                        if line[5] not in user_dict:
                            user_dict[line[5]] = []


                        row = []
                        if line[0] in ["SMSOriginating","SMSTerminating","MSOriginating","MSTerminating"]:
                            if line[0] in ["MSOriginating","MSTerminating"]:
                                row.append("call")
                            else:
                                row.append("text")

                            row.append("out")
                            row.append(line[6])
                            row.append(int(float(line[7])))

                        if len(line[4]):
                            row.append(int(float(line[4])))
                        else:
                            row.append(line[4])

                        dt = line[1] +' '+ line[2]
                        row.append((datetime.strptime(dt, '%Y/%m/%d %H:%M:%S') + timedelta(hours=10)).strftime("%Y-%m-%d %H:%M:%S"))

                        user_dict[line[5]].append(row)
                   if (line[0] in ["SMSTerminating","MSTerminating"]):
                        if line[6] not in user_dict:
                            user_dict[line[6]] = []


                        row = []
                        if line[0] in ["SMSOriginating","SMSTerminating","MSOriginating","MSTerminating"]:
                            if line[0] in ["MSOriginating","MSTerminating"]:
                                row.append("call")
                            else:
                                row.append("text")

                            row.append("in")
                            row.append(line[5])
                            row.append(int(float(line[8])))

                        if len(line[4]):
                            row.append(int(float(line[4])))
                        else:
                            row.append(line[4])

                        dt = line[1] +' '+ line[2]
                        row.append((datetime.strptime(dt, '%Y/%m/%d %H:%M:%S') + timedelta(hours=10)).strftime("%Y-%m-%d %H:%M:%S"))

                        user_dict[line[6]].append(row)

                for u in user_dict:
                    filename = dest_folder + str(u) + ".csv"
                    if not os.path.exists(filename):
                        with open(filename, "a") as f:
                            f.write("interaction,direction,correspondent_id,antenna_id,call_duration,datetime\n")
                    with open(filename, "a") as f:
                        writer = csv.writer(f)
                        writer.writerows(user_dict[u])
                    print filename

    def run():
        for dt in list_dates:
            iterate(dt)


    if __name__ == "__main__":
            if not exists(dest_folder): makedirs(dest_folder)
            run()




with concurrent.futures.ProcessPoolExecutor() as executor:
        for f, zipped_file in zip(listofdate, executor.map(runprocess, listofdate)):
            print("done")


from google.cloud import bigquery
from sklearn.cluster import KMeans
from multiprocessing import Process
from gcloud import storage
from datetime import datetime
import pandas as pd
import math
import numpy as np
import shutil
import os
import glob
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/miguel/google/cred.json"

def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)

def calcPres(listArr, listdates, present, clase_ter, corp, prod, fecha_lan, molecula):  
    km = KMeans(n_clusters=10)
    km.fit(listArr)
    km.predict(listArr)
    labels = km.labels_

    keys = labels
    values = np.reshape(listArr, (len(listArr),))
    listing = list(zip(keys, values, listdates))
    listing.sort(key=lambda x: x[1])

    cent = 0
    acum = 0

    for index, li in enumerate(listing):
        if index == 0:
            cent = li[0] 
        else: 
              if (li[0] != cent):
                cent = li[0] 
                acum = acum+1            
        listing[index] +=(acum,)

    listing.sort(key=lambda x: x[2])

    file = open("/home/miguel/pyAlerts/csv/alerts_{}.csv".format(present.replace('/','').replace('%','').replace('.','').replace(' ','')),"w+")

    for index, li in enumerate(listing):
        if (index % 4 == 0 and index != 0 ):
            if (
              listing[index-4][3] >= listing[index-3][3] and
              listing[index-4][3] < listing[index-2][3] and
              listing[index-4][3] < listing[index-1][3]
            ):

                file.write('ALTA;{};{};{};{};{};{};{};{};\n'.format(clase_ter, corp, prod, fecha_lan, present, molecula, listing[index-4][2][0], listing[index][2][0]))
            elif (
              listing[index-4][3] <= listing[index-3][3] and
              listing[index-4][3] > listing[index-2][3] and
              listing[index-4][3] > listing[index-1][3]
            ): 
                file.write('BAJA;{};{};{};{};{};{};{};{};\n'.format(clase_ter, corp, prod, fecha_lan, present, molecula, listing[index-4][2][0], listing[index][2][0]))
    file.close()



client = storage.Client()
bucket = client.get_bucket('sanfer_alerts')


now = datetime.now()
print("date and time =", now.strftime("%d/%m/%Y %H:%M:%S"))
lineArray = []

client = bigquery.Client()
sql_clases = """
SELECT DISTINCT claseterapeuticanivel3, SPLIT(claseterapeuticanivel3, ' ')[OFFSET(0)] clase FROM sanfer.tb_details_sales  where Corporacion = 'SANFER CORP.'
"""
df_clases = client.query(sql_clases).to_dataframe()
df_clases['clase'].values.tolist()     

for index, row in df_clases.iterrows():
    clase = row['clase']
    
    now = datetime.now()
    print("date and time =", now.strftime("%d/%m/%Y %H:%M:%S"),  '==========================> ', index, clase)
    
    sql_presentation = """
    SELECT DISTINCT presentacion FROM sanfer.tb_details_sales  where claseterapeuticanivel3 like '%{0}%'
    """.format(clase)

    presentations = client.query(sql_presentation).to_dataframe()
    procs = []
    claseTer = []

    sql_pres_months = """
                    SELECT presentacion, mthunidades, fechaventa, claseterapeuticanivel3, corporacion, producto, fechalanzamientopresentacion, moleculan1
                    FROM sanfer.tb_details_sales 
                    where presentacion in ("{0}") 
                    group by presentacion, mthunidades, fechaventa, claseterapeuticanivel3, corporacion, producto, fechalanzamientopresentacion, moleculan1
                    order by presentacion, fechaventa asc""".format('","'.join(presentations['presentacion'].tolist()))
    months = client.query(sql_pres_months).to_dataframe()

    for index, pres in presentations.iterrows():
        present = pres['presentacion']
        months_pres = months.loc[months['presentacion'] == present]
        firsRow = months_pres.iloc[0]
        clase_ter = firsRow['claseterapeuticanivel3']
        corp = firsRow['corporacion']
        prod = firsRow['producto']
        fecha_lan = firsRow['fechalanzamientopresentacion']
        molecula = firsRow['moleculan1']

        listArr = months_pres['mthunidades'].values.reshape(len(months_pres), -1).tolist()
        listdates = months_pres['fechaventa'].values.reshape(len(months_pres), -1).tolist()
        
        proc = Process(target=calcPres, args=(listArr, listdates, present, clase_ter, corp, prod, fecha_lan, molecula))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()


    nameFile = '/home/miguel/pyAlerts/clases/{}.csv'.format(clase)

    with open(nameFile, 'wb') as outfile:
        for filename in glob.glob('/home/miguel/pyAlerts/csv/*.csv'):
            with open(filename, 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)
            os.remove(filename)
    line_prepender(nameFile, 'tendencia;claseterapeutica;corporacion;producto;fechalanzamiento;presentacion;moleculan1;fecini;fecend;tipo')
    
    data = pd.read_csv(nameFile, delimiter=";")
    for index, row in data.iterrows():
        fecin = row['fecini']
        fecen = row['fecend']
        dates = data.loc[(data['fechalanzamiento'] >= fecin) & (data['fechalanzamiento'] <= fecen)]

        if (row['tendencia'] == 'BAJA' and len(dates) > 1):
            data.loc[index, "tipo"]  = 'lanzamiento'
        else:
            combin = data.loc[(data['fecini'] >= fecin) & (data['fecend'] <= fecen)]
            maxi = combin['tendencia'].value_counts().index[0]
            data.loc[index, "tipo"] = '{}, por demanda'.format(maxi)

    data.to_csv('/home/miguel/pyAlerts/process/{}.csv'.format(clase), sep=';', index=False, header=False)
    os.remove(nameFile)
finalname = '/home/miguel/pyAlerts/final/mth.csv'
with open(finalname, 'wb') as outfile:
    for filename in glob.glob('/home/miguel/pyAlerts/process/*.csv'):
        with open(filename, 'rb') as readfile:
            shutil.copyfileobj(readfile, outfile)
        os.remove(filename)

line_prepender(finalname, 'tendencia;claseterapeutica;corporacion;producto;fechalanzamiento;presentacion;moleculan1;fecini;fecend;tipo')
blob = bucket.blob('mth.csv')
blob.upload_from_filename(finalname)

now = datetime.now()
print("date and time =", now.strftime("%d/%m/%Y %H:%M:%S"))
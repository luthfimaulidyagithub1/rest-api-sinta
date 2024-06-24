import os
import sys
sys.path.insert(0, '/home/222011755/myproject/venv/lib/python3.6/site-packages')

import pandas as pd
import numpy as np
import requests, json, os, time
from json import loads, dumps
from io import BytesIO
from flask import Flask, request, jsonify, send_file, Response
from sinta_scraper import Sinta, ResearchScraper, Convert
from pattern_author import PatternAuthor
from integration import MapAuthor, Transformation
from flask_cors import CORS


ALLOWED_EXTENSIONS = {'xlsx','json'}

app = Flask(__name__)
# CORS(app)
cors = CORS(app, resources={r"/*": {"origins": "https://pppm.stis.ac.id"}})
app.config.from_object('config')

sinta = Sinta()

ScrapResearch = ResearchScraper(app.config["API_KEY_ELSEVIER"],
                                app.config["API_KEY_SEMANTIC"],
                                app.config["API_KEY_SPRINGER"])
convert = Convert()
pattern = PatternAuthor()
mapping_author = MapAuthor()
trans = Transformation()

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/delete/<folder>/<filename>', methods =['DELETE'])
def delete_filename(folder,filename):
    directory_path = 'static/data/'+folder
    try:
        if os.path.isfile(os.path.join(directory_path, filename)):
            os.remove(os.path.join(directory_path, filename))
        return {"info":"file "+filename+" di folder "+folder+" berhasil dihapus"}
    except Exception:
        return {"info":"file "+filename+" di folder "+folder+" gagal dihapus"}

@app.route('/delete/<folder>', methods =['GET'])
def delete_file(folder):
    directory_path = 'static/data/'+folder
    try:
        for filename in os.listdir(directory_path):
            if os.path.isfile(os.path.join(directory_path, filename)):
                os.remove(os.path.join(directory_path, filename))
        return {"info":"file di folder "+folder+" berhasil dihapus"}
    except Exception:
        return {"info":"file di folder "+folder+" gagal dihapus"}

@app.route('/list_file/<folder>', methods =['GET'])
def list_file(folder):
    files = []
    directory_path = 'static/data/'+folder
    for filename in os.listdir(directory_path):
        path_file = directory_path+'/'+filename
        ti_m = os.path.getmtime(path_file)
        m_ti = time.ctime(ti_m)
        t_obj = time.strptime(m_ti)
        T_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)
        file = {
            "filename": filename,
            "last_modified": T_stamp
        }
        files.append(file)
    files = json.dumps(files)
    return files

def save_json_file(data,path,filename):
    # delete all files in path
    delete_file(path)
    # save new file in path
    path_data = 'static/data/'+path+'/'
    array_data = [data[i:i+5000] for i in range(0,len(data),5000)] # save file setiap 5000 data
    for i in range(0,len(array_data)):
        num_file = str(i+1)
        path_file = path_data+filename+num_file+'.json'
        out_file = open(path_file, "w") 
        json.dump(array_data[i], out_file, indent = 2) 
        out_file.close() 

@app.route('/read/<path>/<filename>', methods =['GET'])
def read_json(path,filename):
    path_file = 'static/data/'+path+'/'+filename
    file = open(path_file, "r")
    data = json.load(file)
    data = json.dumps(data)
    return data

@app.route('/send_clean',methods =['POST'])
def send_clean():
    content_type = request.headers.get('Content-Type')
    if request.method == 'POST' and (content_type == 'application/json'):
        data = request.json
        df = pd.json_normalize(data)
        data = loads(df.to_json(orient='records'))
        save_json_file(data,'clean','clean')
        return {"success":"perubahan data clean berhasil dikirim"}  
    return {"info":"data is empty"}    

def set_cookies():
    content_type = request.headers.get('Content-Type')
    new_session = requests.session()
    if 'cookies' in request.files:
        cookies = request.files['cookies']
        if cookies and allowed_file(cookies.filename) and cookies.filename!='':
            try:
                cookies = json.load(cookies)
                cookies = requests.utils.cookiejar_from_dict(cookies) 
                new_session.cookies.update(cookies)
            except Exception:
                new_session = False
        else:
            new_session = False
    elif content_type == 'application/json':
        data = request.json
        cookies_data = data.get('sinta_verificator')
        if cookies_data=="":
            new_session = False
        else:
            cookies = {'sinta_verificator':cookies_data}
            cookies = requests.utils.cookiejar_from_dict(cookies) 
            new_session.cookies.update(cookies)
    return new_session

@app.route('/login_sinta', methods =['POST'])
def session_sinta():
    username=''
    password=''

    content_type = request.headers.get('Content-Type')

    if 'username' in request.form and 'password' in request.form:
        username = request.form['username']
        password = request.form['password']
    elif (content_type == 'application/json'):
        data = request.json
        username = data.get('username')
        password = data.get('password')

    sinta = Sinta(username, password) 
    cookies = requests.utils.dict_from_cookiejar(sinta.session.cookies) 
    cookies = str(json.dumps(cookies))

    if request.args.get('download_cookies',None) == 'yes':
        return Response(cookies, 
            mimetype='application/json',
            headers={'Content-Disposition':'attachment;filename=cookies_sinta.json'})
    else:
        return cookies
    
@app.route('/login_sinta/info', methods = ['POST'])
def login_info():
    try:
        new_session = set_cookies()
        if new_session==False:
            return jsonify('no cookies available')
        else:
            return sinta.login_info(new_session)
    except Exception:
        return jsonify('no cookies available')

@app.route('/scrap_link_authors', methods = ['POST'])
def scrap_link_authors(): 
    try:
        new_session = set_cookies()
        if new_session==False:
            return {"info": "no cookies available"}
        else:
            return sinta.scrap_link_author_sinta(new_session)
    except Exception:
        return {"info": "no cookies available"}

@app.route('/scrap/<source>', methods =['POST'])
def scrap_sinta(source):
    try:
        new_session = set_cookies()
        if new_session==False:
            return {"info": "no cookies available"}
        else:
            login = sinta.login_info(new_session)['login']
            if login==False:
                return {"info": "login failed! session expired ! Please login again with correct username and password !"}
            else:
                yf = request.args.get('yf',0)
                yl = request.args.get('yl',0)
                yf = int(yf)
                yl = int(yl)
                if source=='scopus':
                    data = sinta.scrap_scopus(new_session,yf,yl)  
                    if not data.empty:
                        data = data[data['title'].notnull()].drop_duplicates().reset_index(drop=True)
                        data = ScrapResearch.complete_scopus(data)
                        data = convert.convert_scopus(data)
                elif source=='wos':
                    data = sinta.scrap_wos(new_session,yf,yl) 
                    if not data.empty:
                        data = data[data['title'].notnull()].drop_duplicates().reset_index(drop=True)
                        data = ScrapResearch.complete_wos(data)
                        data = convert.convert_wos(data)
                elif source=='garuda':
                    data = sinta.scrap_garuda(new_session,yf,yl)  
                    if not data.empty:
                        data = data[data['title'].notnull()].drop_duplicates().reset_index(drop=True)
                        data = ScrapResearch.complete_garuda(data)
                        data = convert.convert_garuda(data)
                elif source=='google':
                    data = sinta.scrap_google(new_session,yf,yl) 
                    if not data.empty:
                        data = data[data['title'].notnull()].drop_duplicates().reset_index(drop=True)
                        data = ScrapResearch.complete_google(data)
                        data = convert.convert_google(data)
                
                if not data.empty:
                    data_json = loads(data.to_json(orient='records'))
                    save_json_file(data_json,source,source)
                    return data.to_json(orient='records')
                return {"info": "Data "+source+" pada tahun tersebut belum ada."}
    except Exception:
        return {"info": "no cookies available"}

@app.route('/integrasi', methods=['POST'])
def integrasi():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        data =  request.json
        try:
            dosen = data[0]['data_dosen']
            if dosen:
                df_dosen = pd.json_normalize(dosen)
            else:
                return {"info":"data_dosen is empty!"}
        except Exception:
            df_dosen=pd.DataFrame()
        
    elif 'data_dosen' in request.files:
        dosen = request.files['data_dosen']
        if dosen.filename == '':
            res = {"info":"No selected file data_dosen"} 
            return res
        else:
            if dosen and allowed_file(dosen.filename):
                try:
                    df_dosen = pd.read_excel(dosen)
                except Exception:
                    return {"info":"file data_dosen must be .xlsx !"}
            else:
                return {"info":"file data_dosen must be .xlsx !"}
            
    try:
        df_dosen = pattern.pattern_dosen(df_dosen,'fullname')
        fullname = True
    except Exception:
        fullname=False
        return {"info":"column name data_dosen must fullname"}
    
    if fullname==True:
        # SCOPUS
        if (content_type == 'application/json'):
            data =  request.json
            try:
                scopus = data[0]['data_scopus']
                if scopus:
                    df_scopus = pd.json_normalize(scopus)
                else:
                    df_scopus = pd.DataFrame()
            except Exception:
                df_scopus = pd.DataFrame()

        elif 'data_scopus' in request.files:
            scopus = request.files['data_scopus']
            if scopus.filename == '':
                df_scopus = pd.DataFrame()
                res = {"info":"No selected file data_scopus"}
            else:
                if scopus and allowed_file(scopus.filename):
                    try:
                        df_scopus = pd.read_excel(scopus)
                    except Exception:
                        try:
                            df_scopus = pd.read_json(scopus)
                        except Exception:
                            df_scopus=pd.DataFrame()
                else:
                    return {"info":"file data_scopus not allowed ! must be excel or json file !"}
        
        else:
            df_scopus = pd.DataFrame()

        # WOS
        if (content_type == 'application/json'):
            data =  request.json
            try:
                wos = data[0]['data_wos']
                if wos:
                    df_wos = pd.json_normalize(wos)
                else:
                    df_wos = pd.DataFrame()
            except Exception:
                df_wos = pd.DataFrame()

        elif 'data_wos' in request.files:
            wos = request.files['data_wos']
            if wos.filename == '':
                df_wos= pd.DataFrame()
                res = {"info":"No selected file data_wos"} 
            else:
                if wos and allowed_file(wos.filename):
                    try:
                        df_wos = pd.read_excel(wos)
                    except Exception:
                        try:
                            df_wos = pd.read_json(wos)
                        except Exception:
                            df_wos=pd.DataFrame()
                else:
                    return {"info":"file data_wos not allowed ! must be excel or json file !"}
        
        else:
            df_wos = pd.DataFrame()

        # GARUDA
        if (content_type == 'application/json'):
            data =  request.json
            try:
                garuda = data[0]['data_garuda']
                if garuda:
                    df_garuda = pd.json_normalize(garuda)
                else:
                    df_garuda = pd.DataFrame()
            except Exception:
                df_garuda = pd.DataFrame()

        elif 'data_garuda' in request.files:
            garuda = request.files['data_garuda']
            if garuda.filename == '':
                df_garuda = pd.DataFrame()
                res = {"info":"No selected file data_garuda"} 
            else:
                if garuda and allowed_file(garuda.filename):
                    try:
                        df_garuda= pd.read_excel(garuda)
                    except Exception:
                        try:
                            df_garuda= pd.read_json(garuda)
                        except Exception:
                            df_garuda=pd.DataFrame()
                else:
                    return {"info":"file data_garuda not allowed ! must be excel or json file !"}

        else:
            df_garuda = pd.DataFrame()

        # GOOGLE
        if (content_type == 'application/json'):
            data =  request.json
            try:
                google = data[0]['data_google']
                if google:
                    df_google = pd.json_normalize(google)
                else:
                    df_google = pd.DataFrame()
            except Exception:
                df_google = pd.DataFrame()

        elif 'data_google' in request.files:
            google = request.files['data_google']
            if google.filename == '':
                df_google = pd.DataFrame()
                res = {"info":"No selected file data_google"} 
            else:
                if google and allowed_file(google.filename):
                    try:
                        df_google= pd.read_excel(google)
                    except Exception:
                        try:
                            df_google= pd.read_json(google)
                        except Exception:
                            df_google=pd.DataFrame()
                else:
                    return {"info":"file data_google not allowed ! must be excel or json file !"}
        
        else:
            df_google = pd.DataFrame()

        # filter and match scopus
        try:
            df_scopus = mapping_author.map_author_sinta(df_scopus,'author_sinta',df_dosen)
            list_dosen=df_scopus.apply(lambda x: mapping_author.map_scopus(str(x['authors']),df_dosen,x['author_sinta']), axis=1)
            df_scopus['author_dosen'] = list_dosen[0]
            df_scopus['nama_dosen'] = list_dosen[1]
        except Exception:
            df_scopus=pd.DataFrame()
        
        # filter and match wos
        try:
            df_wos = mapping_author.map_author_sinta(df_wos,'author_sinta',df_dosen)
            list_dosen=df_wos.apply(lambda x: mapping_author.map_wos(str(x['authors']),df_dosen,x['author_sinta']), axis=1)
            df_wos['author_dosen'] = list_dosen[0]
            df_wos['nama_dosen'] = list_dosen[1]
        except Exception:
            df_wos=pd.DataFrame()

        # filter and match garuda
        try:
            df_garuda = mapping_author.map_author_sinta(df_garuda,'author_sinta',df_dosen)
            list_dosen=df_garuda.apply(lambda x: mapping_author.map_garuda(str(x['authors']),df_dosen,x['author_sinta']), axis=1)
            df_garuda['author_dosen'] = list_dosen[0]
            df_garuda['nama_dosen'] = list_dosen[1]
        except Exception:
            df_garuda=pd.DataFrame()

        # filter and match google
        try:
            df_google = mapping_author.map_author_sinta(df_google,'author_sinta',df_dosen)
            list_dosen=df_google.apply(lambda x: mapping_author.map_google(str(x['authors']),df_dosen,x['author_sinta']), axis=1)
            df_google['author_dosen'] = list_dosen[0]
            df_google['nama_dosen'] = list_dosen[1]
        except Exception:
            df_google=pd.DataFrame()

        df = pd.concat([df_scopus, df_wos, df_garuda, df_google], ignore_index=True, sort=False)
        if not df.empty:
            df = trans.filter_paper(df)
            data = loads(df.to_json(orient='records'))
            save_json_file(data,'integrasi','integrasi')
            return df.to_json(orient='records')
        return {"info":"No data is integrated"}       
    return {"info":"There's no data dosen"}

@app.route('/cleaning', methods=['POST'])
def cleaning():
    content_type = request.headers.get('Content-Type')
    t = request.args.get('threshold',None)

    if request.method == 'POST' and 'file' in request.files:
        file = request.files['file']
        if file and allowed_file(file.filename) and file.filename!='':
            try:
                df = pd.read_excel(file)
            except Exception:
                df = pd.read_json(file)
        else:
            return {"info":"data is empty"}
    elif request.method == 'POST' and (content_type == 'application/json'):
        data = request.json
        try:
            df = pd.json_normalize(data)
        except Exception:
            return {"info":"data is empty"}
    try:
        if t == None:
            df = trans.cleaning_data(df,0.8)
        else:
            t = float(t)
            df = trans.cleaning_data(df,t)
        df = trans.klasifikasi_paper(df)
        data = loads(df.to_json(orient='records'))
        save_json_file(data,'clean','clean')
        return df.to_json(orient='records') 
    except Exception:
        return {"info":"incorrect or missing data columns"}

@app.route('/merge_data', methods=['POST'])
def merge_data():
    content_type = request.headers.get('Content-Type')
    if request.method == 'POST' and (content_type == 'application/json'):
        data = request.json
        res = []
        df = pd.json_normalize(data)
        # nunique group_flag
        ngroup_flag = df['group_data'].unique()
        for i in ngroup_flag:
            data = df.loc[df['group_data']==i]
            data = trans.merge_data(data)
            data = trans.klasifikasi_paper(data)
            data = data.to_dict(orient='records')
            res.append(data[0])
        res = json.dumps(res)
        return res
    else:
        return {"info":"data is empty"}
    
@app.route('/data_by_authors', methods=['POST'])
def data_by_authors():
    content_type = request.headers.get('Content-Type')
    if request.method == 'POST' and (content_type == 'application/json'):
        data = request.json
        df = pd.json_normalize(data)
        df = trans.research_by_author(df)
        return df.to_json(orient='records') 
    else:
        return {"info":"data is empty"}

@app.route('/klasifikasi', methods=['POST'])
def klasifikasi():
    content_type = request.headers.get('Content-Type')
    if request.method == 'POST' and (content_type == 'application/json'):
        data = request.json
        df = pd.json_normalize(data)
        df = trans.klasifikasi_paper(df)
        data = loads(df.to_json(orient='records'))
        save_json_file(data,'clean','clean')
        return df.to_json(orient='records')
    else:
        return {"info":"data is empty"}

@app.route('/summary', methods=['POST'])
def summary():
    content_type = request.headers.get('Content-Type')
    if request.method == 'POST' and (content_type == 'application/json'):
        data = request.json
        df = pd.json_normalize(data)
        df = trans.summary_paper(df)
        return df.to_json(orient='records') 
    else:
        return {"info":"data is empty"}

@app.route('/abstract_url', methods=['POST'])
def abstract_url():
    content_type = request.headers.get('Content-Type')
    if request.method == 'POST' and 'url' in request.form:
        url = request.form['url']
    elif (content_type == 'application/json'):
        data = request.json
        url = data.get('url')
    abstract = ScrapResearch.abstract_url(url)
    res = {'url': url,
           'abstract': abstract}
    return res

@app.route('/abstract_pdf', methods=['POST'])
def abstract_pdf():
    content_type = request.headers.get('Content-Type')
    if request.method == 'POST' and 'url' in request.form:
        url = request.form['url']
    elif (content_type == 'application/json'):
        data = request.json
        url = data.get('url')
    abstract = ScrapResearch.abstract_pdf(url)
    res = {'url': url,
           'abstract': abstract}
    return res

@app.route('/', methods=['GET']) 
def home(): 
    return "Welcome to REST API Sinta"

if __name__ == '__main__':
    app.run(host="0.0.0.0") # flask --app main.py --debug run

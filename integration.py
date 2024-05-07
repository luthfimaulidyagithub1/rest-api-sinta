import pandas as pd
import Levenshtein as levenshtein
from googletrans import Translator

translator = Translator()

class MapAuthor(object):
    def map_author_sinta(self,df,col_author_sinta,dosen):
        df['is_dosen'] = df[col_author_sinta].isin(dosen['pattern2'])
        df = df.loc[df['is_dosen']==True].reset_index(drop=True)
        return df

    def list_pattern(self,author,pattern,dosen):
        list_dosen=[]
        list_dosen.append(author[0])
        list_new = dosen[dosen[pattern].isin(list_dosen)]
        nama_lengkap = list_new['pattern2'][list_new.index.tolist()[0]]
        author = author[0]
        return nama_lengkap, author

    def map_dosen(self,author_list,dosen,author_sinta):
        dosen_stis=[]
        nama_dosen=[]
        for i in author_list:
            for num in range(1,12):
                pattern = 'pattern'+str(num)
                if author_list[i].isin(dosen[pattern])[0]==True:
                    nama_lengkap, author = self.list_pattern(author_list[i],pattern,dosen)
                    nama_dosen.append(nama_lengkap)
                    dosen_stis.append(author)
        nama_dosen.append(author_sinta)
        dosen_stis.append(author_sinta)
        nama_dosen = set(nama_dosen)
        nama_dosen = '; '.join(nama_dosen)
        dosen_stis = set(dosen_stis)
        dosen_stis = '; '.join(dosen_stis)

        return pd.Series([dosen_stis,nama_dosen])

    def map_scopus(self,authors,dosen,author_sinta):
        author_list = pd.Series(authors).str.split("; ", expand=True)
        return self.map_dosen(author_list,dosen,author_sinta)

    def map_wos(self,authors,dosen,author_sinta):
        author_list = pd.Series(authors).str.split("; ", expand=True).replace(';','')
        return self.map_dosen(author_list,dosen,author_sinta)

    def map_garuda(self,authors,dosen,author_sinta):
        author_list = pd.Series(authors).str.split("; ", expand=True)
        return self.map_dosen(author_list,dosen,author_sinta)

    def map_google(self,authors,dosen,author_sinta):
        author_list = author_list = pd.Series(authors).str.split(", ", expand=True)
        if author_list.iloc[:,-1][0]=='...':
            flag_google_author = 1
        else:
            flag_google_author = 0
        if " dan " in author_list[0][0]:
            author_list2 = pd.Series(author_list[0][0]).str.split(" dan ", expand=True)
            author_list = pd.concat([author_list2,author_list.iloc[: , 1:]], axis=1)
            author_list.columns = range(author_list.columns.size)
        if " and " in author_list[0][0]:
            author_list2 = pd.Series(author_list[0][0]).str.split(" and ", expand=True)
            author_list = pd.concat([author_list2, author_list.iloc[: , 1:]], axis=1)
            author_list.columns = range(author_list.columns.size)

        dosen_stis, nama_dosen = self.map_dosen(author_list,dosen,author_sinta)
        return pd.Series([dosen_stis, nama_dosen, flag_google_author])

class Transformation(object):

    def merge_data_duplicate(self,df):
      if df['author_sinta'].nunique()==1:
        author_sinta=df['author_sinta'][0]
      else:
        list_author_sinta=[]
        for i in range(0,len(df)):
          author_sinta = df['author_sinta'][i].split("; ")
          list_author_sinta.extend(author_sinta)
        author_sinta = '; '.join(list(set(list_author_sinta)))

      list_nama_dosen=[] # setelah match dosen
      for i in range(0,len(df)):
        nama_dosen = df['nama_dosen'][i].split("; ")
        list_nama_dosen.extend(nama_dosen) # setelah match dosen
      nama_dosen = '; '.join(list(set(list_nama_dosen))) # setelah match dosen

      id = df['id'][0]
      judul = df['judul'][0]
      lang_judul = df['lang_judul'][0]
      link = df['link'][0]
      doi = df['doi'][0]
      doi_info = df['doi_info'][0]
      authors = df['authors'][0]
      jumlah_sitasi = df.loc[df['jumlah_sitasi'].notnull()]['jumlah_sitasi'].max()
      
      abstrak=None
      abstrak_info=None
      lang_abstrak=None
      for i in range(0, len(df)):
        if df['abstrak'].notnull()[i]:
          abstrak = df['abstrak'][i]
          abstrak_info = df['abstrak_info'][i]
          lang_abstrak = df['lang_abstrak'][i]
          break
        else:
          continue

      nama_publikasi = None
      for i in range(0, len(df)):
        if df['nama_publikasi'].notnull()[i]:
          nama_publikasi = df['nama_publikasi'][i]
          break
        else:
          continue
      
      publisher = None
      for i in range(0, len(df)):
        if df['publisher'].notnull()[i]:
          publisher = df['publisher'][i]
          break
        else:
          continue
      
      rank=[]
      for i in range(0,len(df)):
        if df['rank'].notnull()[i] and df['rank'][i]!='?' and df['rank'][i]!='-':
          rank.append(df['rank'][i])
      rank=', '.join(list(set(rank)))
      if rank=='':
        rank=None

      tahun = None
      for i in range(0, len(df)):
        if df['tahun'].notnull()[i]:
          tahun = df['tahun'][i]
          break
        else:
          continue
      
      tipe=None
      for i in range(0, len(df)):
        if df['tipe'][i]=='journal article':
          tipe = df['tipe'][i]
          break
        else:
          continue
      if tipe==None:
        for i in range(0, len(df)):
          if df['tipe'].notnull()[i]:
            tipe = df['tipe'][i]
            break
          else:
            continue

      df = pd.DataFrame()
      df['id'] = [id]
      df['author_sinta'] = [author_sinta]
      df['judul'] = [judul]
      df['abstrak'] = [abstrak]
      df['doi'] = [doi]
      df['authors'] = [authors]
      df['nama_publikasi'] = [nama_publikasi]
      df['publisher'] = [publisher]
      df['tipe'] = [tipe]
      df['rank'] = [rank]
      df['link'] = [link]
      df['tahun'] = [tahun]
      df['jumlah_sitasi'] = [jumlah_sitasi]
      df['abstrak_info'] = [abstrak_info]
      df['doi_info'] = [doi_info]
      df['lang_judul'] = [lang_judul]
      df['lang_abstrak'] = [lang_abstrak]
      
      # setelah match dosen
      df['nama_dosen'] = [nama_dosen]

      return df

    def clean_data_by_doi(self,df):
      df['doi'] = df['doi'].str.lower()
      df['id'] = df.index+1
      df_doi_notnull = df.loc[df['doi'].notnull()].reset_index(drop=True)
      df_doi_isnull = df.loc[df['doi'].isnull()].reset_index(drop=True)
      df_doi_notnull['duplicate'] = df_doi_notnull.duplicated('doi',keep=False)
      df_doi_notnull['DuplicateGroup'] = df_doi_notnull.groupby(list(df_doi_notnull['doi'])).ngroup()
      df_doi_noduplicate = df_doi_notnull.loc[df_doi_notnull['duplicate']==False].reset_index(drop=True).drop(columns=['duplicate','DuplicateGroup'], axis=1)
      df_doi_duplicate = df_doi_notnull.loc[df_doi_notnull['duplicate']==True & df_doi_notnull['doi'].notnull()].sort_values(by=['DuplicateGroup','id']).reset_index(drop=True)

      df = pd.DataFrame()
      for i in df_doi_duplicate['DuplicateGroup'].unique():
        data = df_doi_duplicate.loc[df_doi_duplicate['DuplicateGroup']==i].reset_index(drop=True)
        data = self.merge_data_duplicate(data) # add self
        df = pd.concat([df,data], ignore_index=True, sort=False)
      df = pd.concat([df,df_doi_noduplicate,df_doi_isnull], ignore_index=True, sort=False)
      df['id'] = df.index+1
      df = df.drop(columns=['is_dosen','author_dosen'], axis=1)

      return df

    def cleaning(self,df):
      df = df[df['judul'].notnull()].drop_duplicates(subset=df.columns.difference(['id'])).reset_index(drop=True)
      df = df[(df['tipe']=='journal article') | (df['tipe']=='conference proceeding article') | (df['tipe']=='conference article') | (df['tipe']=='proceeding article') | (df['tipe']=='seminar article') | (df['tipe']=='procedia article')| (df['tipe']=='review') | (df['tipe']=='case report') | (df['tipe']=='peer review') | (df['tipe'].isnull())].reset_index(drop=True)
      # df['id'] = df.index+1
      df = self.clean_data_by_doi(df)
      df['judul_indo']=None


      for i in range(0,len(df['judul'])):
        if df['lang_judul'][i] == 'en' or (df['lang_judul'][i] != 'id' and df['lang_abstrak'][i] == 'en'):
          try:
            df['judul_indo'][i] = translator.translate(df['judul'][i] , dest='id').text
          except Exception:
            df['judul_indo'][i] = None
            
      return df
    
    def check_redundant_data(self,df,threshold):
      df_judul_indo_isnull = df.loc[df['judul_indo'].isnull()].reset_index(drop=True)
      df_judul_indo_notnull = df.loc[df['judul_indo'].notnull()].reset_index(drop=True)
      for i in range(0,len(df_judul_indo_isnull)):
        if (df_judul_indo_isnull['lang_judul'][i] == 'en') or (df_judul_indo_isnull['lang_judul'][i] != 'id' and df_judul_indo_isnull['lang_abstrak'][i] == 'en'):
          try:
            df_judul_indo_isnull['judul_indo'][i] = translator.translate(df_judul_indo_isnull['judul'][i] , dest='id').text
          except Exception:
            df_judul_indo_isnull['judul_indo'][i] = None
      df = pd.concat([df_judul_indo_notnull,df_judul_indo_isnull], ignore_index=True).sort_values(by=['id'])
      df = df.reset_index(drop=True)
  
      df['id_paper'] = None
      df['flag'] = None
      df['id']=df.index+1

      df_doi_isnull = df[df['doi'].isnull()]
      
      for i in range(0,len(df)):
        list_id=None
        list_ratio=None
        for j in range(df_doi_isnull.index[0],df_doi_isnull.index[-1]+1):
          if i!=j:
            # if df['doi'].notnull()[i] and df['doi'].notnull()[j]:
            #   paper=False
            # elif df['tahun'][i]!=df['tahun'][j]:
            # # str(df['tahun'][i]).split(' ')[-1]!=str(df['tahun'][j]).split(' ')[-1]:
            #   paper=False
            if (df['tahun'][i]==df['tahun'][j]) and (str(df['nama_dosen'][i]) == str(df['nama_dosen'][j])):
              pub_i = df['nama_publikasi'][i]
              pub_j = df['nama_publikasi'][j]
              if pub_i==None:
                pub_i=''
              if pub_j==None:
                pub_j=''
              string1 = df['judul'][i]+' '+pub_i
              string2 = df['judul'][j]+' '+pub_j

              if df['lang_judul'][i] != 'id' and df['lang_judul'][j] == 'id':
                if df['judul_indo'][i] !=None:
                  judul_i = df['judul_indo'][i]
                  string1 = judul_i+' '+pub_i
              elif df['lang_judul'][i] == 'id' and df['lang_judul'][j] != 'id':
                if df['judul_indo'][j] !=None:
                  judul_j = df['judul_indo'][j]
                  string2 = judul_j+' '+pub_j

              lv_ratio = levenshtein.ratio(string1.lower(), string2.lower())
              string1 = df['judul'][i]+' '+pub_i

              if lv_ratio >= threshold and i!=j:
                id=[]
                id.append(df['id'][i])
                id.append(df['id'][j])
                id.sort()
                
                # list_id.append(id)
                if list_id==None:
                  list_id = str(id)
                  list_ratio = str(round(lv_ratio,2))
                else:
                  list_id = list_id+';'+str(id)
                  list_ratio = list_ratio+';'+str(round(lv_ratio,2))
                  
        df['id_paper'][i] = list_id
        df['flag'][i] = list_ratio
        df['group_data']=None

      try:
        df = df.sort_values('id_paper')
        df_flag_notnull = df.loc[df['id_paper'].notnull()].reset_index(drop=True)
        df_flag_null = df.loc[df['id_paper'].isnull()].reset_index(drop=True)
        df_flag_notnull['temp'] = None
        for i in range(0,len(df_flag_notnull['id'])):
          df_flag_notnull['temp'][i] = df_flag_notnull['id_paper'][i].split()[0]

        df1=pd.DataFrame()
        j=1
        for i in range(0,len(df)):
          string = "\["+str(i)+","
          df_new = df_flag_notnull.loc[df_flag_notnull['temp'].str.contains(string)]
          if not df_new.empty:
            df_new['group_data'] = j
            df1 = pd.concat([df1,df_new], ignore_index=True, sort=False)
            j=j+1

        df = pd.concat([df1,df_flag_null], ignore_index=True, sort=False).drop('temp', axis=1)
        return df.sort_values('group_data')
      except Exception:
        return df

    def merge_data(self,df):
      id = df['id'][0]
      group_data = df['group_data'][0]

      list_flag_paper=[]
      for i in range(0,len(df['id'])):
          list_id_paper=df['id_paper'][i].split(";")
          list_flag=df['flag'][i].split(";")
          for j in range(0, len(list_id_paper)):
            item_flag = list_id_paper[j]+"-"+list_flag[j]
            list_flag_paper.append(item_flag)
      list_flag_paper = list(set(list_flag_paper))
      for i in range(1,len(df['id'])):
          car_id1 = "["+str(df['id'][i])+","
          car_id2 = ", "+str(df['id'][i])+"]"
          list_flag_paper = [ x for x in list_flag_paper if car_id1 not in x ]
          list_flag_paper = [ x for x in list_flag_paper if car_id2 not in x ]
      
      id_paper=[]
      flag=[]
      for i in range(0, len(list_flag_paper)):
        id_paper.append(list_flag_paper[i].split("-")[0])
        flag.append(list_flag_paper[i].split("-")[1])
      id_paper = ';'.join(id_paper)
      flag = ';'.join(flag)

      if id_paper=='':
        id_paper=None
        flag=None
        group_data=None

      if df['author_sinta'].nunique()==1:
        author_sinta=df['author_sinta'][0]
      else:
        list_author_sinta=[]
        for i in range(0,len(df)):
          author_sinta = df['author_sinta'][i].split("; ")
          list_author_sinta.extend(author_sinta)
        author_sinta = '; '.join(list(set(list_author_sinta)))

      list_nama_dosen=[] # setelah match dosen
      for i in range(0,len(df)):
        nama_dosen = df['nama_dosen'][i].split("; ")
        list_nama_dosen.extend(nama_dosen) # setelah match dosen
      nama_dosen = '; '.join(list(set(list_nama_dosen))) # setelah match dosen

      judul = df['judul'][0]
      lang_judul = df['lang_judul'][0]
      link = df['link'][0]
      # doi = df['doi'][0]
      # doi_info = df['doi_info'][0]
      # authors = df['authors'][0]
      jumlah_sitasi = df.loc[df['jumlah_sitasi'].notnull()]['jumlah_sitasi'].max()
      
      doi=None
      doi_info=None
      authors=None
      for i in range(0, len(df)):
        if df['doi'].notnull()[i]:
          doi = df['doi'][i]
          doi_info = df['doi_info'][i]
          authors = df['authors'][i]
      if authors==None:
        authors = df['authors'][i]

      abstrak=None
      abstrak_info=None
      lang_abstrak=None
      for i in range(0, len(df)):
        if df['abstrak'].notnull()[i]:
          abstrak = df['abstrak'][i]
          abstrak_info = df['abstrak_info'][i]
          lang_abstrak = df['lang_abstrak'][i]

      nama_publikasi = None
      for i in range(0, len(df)):
        if df['nama_publikasi'].notnull()[i]:
          nama_publikasi = df['nama_publikasi'][i]
      
      publisher = None
      for i in range(0, len(df)):
        if df['publisher'].notnull()[i]:
          publisher = df['publisher'][i]
      
      rank=[]
      for i in range(0,len(df)):
        if df['rank'].notnull()[i] and df['rank'][i]!='?' and df['rank'][i]!='-':
          rank.append(df['rank'][i])
      rank=', '.join(list(set(rank)))
      if rank=='':
        rank=None

      tahun = None
      for i in range(0, len(df)):
        if df['tahun'].notnull()[i]:
          tahun = df['tahun'][i]
      
      tipe=None
      for i in range(0, len(df)):
        if df['tipe'][i]=='journal article':
          tipe = df['tipe'][i]
        else:
          tipe=None
      if tipe==None:
        for i in range(0, len(df)):
          if df['tipe'].notnull()[i]:
            tipe = df['tipe'][i]

      judul_indo = None
      for i in range(0, len(df)):
        if df['judul_indo'].notnull()[i]:
          tahun = df['judul_indo'][i]

      res = {'id':str(id),
             'author_sinta':author_sinta,
             'judul':judul,
              'abstrak':abstrak,
              'doi':doi,
              'authors':authors,
              'nama_publikasi':nama_publikasi,
              'publisher':publisher,
              'rank':rank,
              'link':link,
              'tahun':str(tahun),
              'jumlah_sitasi':jumlah_sitasi,
              'abstrak_info':abstrak_info,
              'doi_info':doi_info,
              'lang_judul':lang_judul,
              'lang_abstrak':lang_abstrak,
              'nama_dosen':nama_dosen,
              'judul_indo':judul_indo,
              'id_paper': id_paper,
              'flag': flag,
              'group_data': group_data}

      return res

# BELUM FIX
    def flag_paper(self,df,threshold):

      df = df[df['judul']!=''].drop_duplicates(subset=df.columns.difference(['id'])).reset_index(drop=True)
      # tambahin drop duplicate by kolom doi
      df['id'] = df.index+1
      # cols = list(df)
      # move the column to head of list using index, pop and insert
      # cols.insert(0, cols.pop(cols.index('id')))
      # df = df.loc[:, cols]

      df['id_paper']=None
      df['flag']=None
      df['cat_flag']=None

      for i in range(0,len(df['judul'])):
        if (df['judul_indo'].isnull()[i] and df['lang_judul'][i] == 'en') or (df['judul_indo'].isnull()[i] and df['lang_judul'][i] != 'id' and df['lang_abstrak'][i] == 'en'):
          try:
            df['judul_indo'][i] = translator.translate(df['judul'][i] , dest='id').text
          except Exception:
            df['judul_indo'][i] = None

      for i in range(0,len(df['judul'])):
        # list_id=[]
        list_id=None
        list_ratio=None
        list_cat_flag=None
        judul1 = df['judul'][i]
        for j in range(0,len(df['judul'])):
          judul2 = df['judul'][j]
          if df['lang_judul'][i] != 'id' and df['lang_judul'][j] == 'id':
            if df['judul_indo'][i] !=None:
              judul1 = df['judul_indo'][i]
          elif df['lang_judul'][i] == 'id' and df['lang_judul'][j] != 'id':
            if df['judul_indo'][j] !=None:
              judul2 = df['judul_indo'][j]

          lv_ratio = levenshtein.ratio(judul1.lower(), judul2.lower())
          judul1 = df['judul'][i]
          
          if lv_ratio >= threshold and i!=j:
            id=[]
            id.append(df['id'][i])
            id.append(df['id'][j])
            id.sort()

            flag_judul = round(lv_ratio,2)
            if (flag_judul==1) and (df['author_sinta'][i]==df['author_sinta'][j]):
              cat_flag = 'judul sama author_sinta sama'
            elif (flag_judul==1) and (df['author_sinta'][i]!=df['author_sinta'][j]):
              cat_flag = 'judul sama author_sinta beda'
            elif (flag_judul!=1) and (df['author_sinta'][i]==df['author_sinta'][j]):
              cat_flag = 'judul mirip author_sinta sama'
            elif (flag_judul!=1) and (df['author_sinta'][i]!=df['author_sinta'][j]):
              cat_flag = 'judul mirip author_sinta beda'
              
            # list_id.append(id)
            if list_id==None:
              list_id = str(id)
              list_ratio = str(round(lv_ratio,2))
              list_cat_flag = cat_flag
            else:
              list_id = list_id+';'+str(id)
              list_ratio = list_ratio+';'+str(round(lv_ratio,2))
              list_cat_flag = list_cat_flag+';'+cat_flag
            # list_ratio.append(round(lv_ratio,2))

        df['id_paper'][i] = list_id
        df['flag'][i] = list_ratio
        df['cat_flag'][i] = list_cat_flag

      df = df.sort_values('id_paper')
      df_flag_notnull = df.loc[df['id_paper'].notnull()].reset_index(drop=True)
      df_flag_null = df.loc[df['id_paper'].isnull()].reset_index(drop=True)
      df_flag_notnull['temp'] = None
      for i in range(0,len(df_flag_notnull['id'])):
        df_flag_notnull['temp'][i] = df_flag_notnull['id_paper'][i].split()[0]

      df1=pd.DataFrame()
      j=1
      for i in range(0,len(df['id'])):
        string = "\["+str(i)+","
        df_new = df_flag_notnull.loc[df_flag_notnull['temp'].str.contains(string)]
        if not df_new.empty:
          df_new['group_flag'] = j
          df1 = pd.concat([df1,df_new], ignore_index=True, sort=False)
          j=j+1

      df = pd.concat([df1,df_flag_null], ignore_index=True, sort=False).drop('temp', axis=1)

      return df.sort_values('group_flag')

    def merge_data_past(self,df):
      df = df.sort_values(['link', 'doi_info', 'abstrak_info'], ascending=[True, True, True]).reset_index(drop=True)
      id = df['id'][0]

      list_flag_paper=[]
      for i in range(0,len(df['id'])):
          list_id_paper=df['id_paper'][i].split(";")
          list_flag=df['flag'][i].split(";")
          list_cat_flag=df['cat_flag'][i].split(";")
          for j in range(0, len(list_id_paper)):
            item_flag = list_id_paper[j]+"-"+list_flag[j]+"-"+list_cat_flag[j]
            list_flag_paper.append(item_flag)
      list_flag_paper = list(set(list_flag_paper))
      for i in range(1,len(df['id'])):
          car_id1 = "["+str(df['id'][i])+","
          car_id2 = ", "+str(df['id'][i])+"]"
          list_flag_paper = [ x for x in list_flag_paper if car_id1 not in x ]
          list_flag_paper = [ x for x in list_flag_paper if car_id2 not in x ]
      
      id_paper=[]
      flag=[]
      cat_flag=[]
      for i in range(0, len(list_flag_paper)):
        id_paper.append(list_flag_paper[i].split("-")[0])
        flag.append(list_flag_paper[i].split("-")[1])
        cat_flag.append(list_flag_paper[i].split("-")[2])
      id_paper = ';'.join(id_paper)
      flag = ';'.join(flag)
      cat_flag = ';'.join(cat_flag)
      group_flag = df['group_flag'][0]

      if id_paper=='':
        id_paper=None
        flag=None
        cat_flag=None
        group_flag=None

      if df['author_sinta'].nunique()==1:
        author_sinta=df['author_sinta'][0]
      else:
        author_sinta=None
        list_author_sinta=[]
        for i in range(0,len(df['id'])):
          list_author_sinta=list_author_sinta+df['author_sinta'][i].split("; ")
        author_sinta = '; '.join(list(set(list_author_sinta)))
      
      for i in range(0,len(df['id'])):
        doi_info = df['doi_info'][i]
        # index=None
        abstrak=None
        abstrak_info=None
        lang_abstrak=None

        if doi_info=='garuda':
          abstrak=df['abstrak'][i]
          # index=i
          doi_info = df['doi_info'][i]
          abstrak_info = df['abstrak_info'][i]
          lang_abstrak = df['lang_abstrak'][i]
          if df['abstrak'].isnull()[i]:
            continue
          else:
            break

        if doi_info=='elsevier':
          if df['abstrak_info'][i]=='elsevier':
            abstrak=df['abstrak'][i]
            # index=i
            doi_info = df['doi_info'][i]
            abstrak_info = df['abstrak_info'][i]
            lang_abstrak = df['lang_abstrak'][i]
            if df['abstrak'].isnull()[i]:
              continue
            else:
              break
          if df['abstrak_info'][i]=='springer':
            abstrak=df['abstrak'][i]
            # index=i
            doi_info = df['doi_info'][i]
            abstrak_info = df['abstrak_info'][i]
            lang_abstrak = df['lang_abstrak'][i]
            if df['abstrak'].isnull()[i]:
              continue
            else:
              break
          if df['abstrak_info'][i]=='semantic':
            abstrak=df['abstrak'][i]
            # index=i
            doi_info = df['doi_info'][i]
            abstrak_info = df['abstrak_info'][i]
            lang_abstrak = df['lang_abstrak'][i]
            if df['abstrak'].isnull()[i]:
              continue
            else:
              break

        if doi_info!='garuda' and doi_info!='elsevier':
          if df['abstrak_info'][i]=='springer':
            abstrak=df['abstrak'][i]
            # index=i
            doi_info = df['doi_info'][i]
            abstrak_info = df['abstrak_info'][i]
            lang_abstrak = df['lang_abstrak'][i]
            if df['abstrak'].isnull()[i]:
              continue
            else:
              break
          if df['abstrak_info'][i]=='semantic':
            abstrak=df['abstrak'][i]
            # index=i
            doi_info = df['doi_info'][i]
            abstrak_info = df['abstrak_info'][i]
            lang_abstrak = df['lang_abstrak'][i]

      for i in range(0,len(df['id'])):
        doi=None
        if df['doi_info'][i]=='garuda' and df['doi'].notnull()[i]:
          doi=df['doi'][i]
          break
        if df['doi_info'][i]=='elsevier':
          doi=df['doi'][i]
          break
        if df['doi_info'][i]=='springer':
          doi=df['doi'][i]
          break
        if df['doi_info'][i]=='semantic':
          doi=df['doi'][i]
      len1 = 0
      for i in range(0,len(df['id'])):
        if len(df['nama_dosen'][i].split(";"))>len1:
          len1 = len(df['nama_dosen'][i].split(";"))
          authors = df['authors'][i]

      list_dosen=[]
      for i in range(0,len(df['id'])):
        list_dosen=list_dosen+df['nama_dosen'][i].split("; ")
      nama_dosen = '; '.join(list(set(list_dosen)))

      nama_publikasi=None
      try:
        pub = df[~df['link'].str.contains('scholar.google')].reset_index(drop=True)
        for i in range(0,len(pub['id'])):
          if pub['nama_publikasi'].isnull()[i]:
            continue
          else:
            nama_publikasi=pub['nama_publikasi'][i]
            break
      except Exception:
        nama_publikasi=None
      if nama_publikasi==None:
        for i in range(0,len(df['id'])):
          if df['nama_publikasi'].isnull()[i]:
            continue
          else:
            nama_publikasi=df['nama_publikasi'][i]
            break

      for i in range(0,len(df['id'])):
        publisher=None
        if df['publisher'].isnull()[i]:
          continue
        else:
          publisher=df['publisher'][i]
          break
      rank=[]
      for i in range(0,len(df['id'])):
        if df['rank'].notnull()[i] and df['rank'][i]!='?' and df['rank'][i]!='-':
          rank.append(df['rank'][i])
      rank=','.join(list(set(rank)))
      if rank=='':
        rank=None
      try:
        df_link = df[df['link'].str.contains('scopus')].reset_index(drop=True)
        link = df_link['link'][0]
        tahun = df_link['tahun'][0]
        judul=df_link['judul'][0]
        lang_judul=df_link['lang_judul'][0]
      except Exception:
        try:
          df_link = df[df['link'].str.contains('webofscience')].reset_index(drop=True)
          link=df_link['link'][0]
          tahun = df_link['tahun'][0]
          judul=df_link['judul'][0]
          lang_judul=df_link['lang_judul'][0]
        except Exception:
          try:
            df_link = df[df['link'].str.contains('garuda')].reset_index(drop=True)
            link=df_link['link'][0]
            tahun = df_link['tahun'][0]
            judul=df_link['judul'][0]
            lang_judul=df_link['lang_judul'][0]
          except Exception:
            df_link = df[df['link'].str.contains('google')].reset_index(drop=True)
            link = df_link['link'][0]
            tahun = df_link['tahun'][0]
            for t in range(0,len(df_link['link'])):
              if df_link['tahun'][t]=='0000':
                continue
              else:
                tahun = df_link['tahun'][t]
                break
            judul=df_link['judul'][0]
            lang_judul=df_link['lang_judul'][0]

      jumlah_sitasi=0

      for i in range(0,len(df['id'])):
        if df['jumlah_sitasi'].notnull()[i] and int(df['jumlah_sitasi'][i])>jumlah_sitasi:
          jumlah_sitasi=int(df['jumlah_sitasi'][i])

      for i in range(0,len(df['id'])):
        if df['judul_indo'].notnull()[i]:
          judul_indo = df['judul_indo'][i]
          break
        else:
          judul_indo=None

      res = {'id':str(id),
             'author_sinta':author_sinta,
             'judul':judul,
              'abstrak':abstrak,
              'doi':doi,
              'authors':authors,
              'nama_publikasi':nama_publikasi,
              'publisher':publisher,
              'rank':rank,
              'link':link,
              'tahun':str(tahun),
              'jumlah_sitasi':jumlah_sitasi,
              'abstrak_info':abstrak_info,
              'doi_info':doi_info,
              'lang_judul':lang_judul,
              'lang_abstrak':lang_abstrak,
              'nama_dosen':nama_dosen,
              'judul_indo':judul_indo,
              'id_paper': id_paper,
              'flag': flag,
              'cat_flag': cat_flag,
              'group_flag': group_flag}
      return res

    def research_by_author(self, df,nama_dosen):
      df[nama_dosen]=df[nama_dosen].str.split('; ')
      df=df.explode(nama_dosen).reset_index(drop=True)
      return df
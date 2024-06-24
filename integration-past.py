import numpy as np
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
        
        nama_dosen = list(set(nama_dosen))
        nama_dosen.sort()
        # nama_dosen = set(nama_dosen)
        nama_dosen = '; '.join(nama_dosen)
        
        dosen_stis = list(set(dosen_stis))
        dosen_stis.sort()
        # dosen_stis = set(dosen_stis)
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
    # INTEGRASI
    def filter_paper(self,df):
      df = df[df['judul'].notnull()].drop_duplicates(subset=df.columns.difference(['id'])).reset_index(drop=True)
      df = df[(df['tipe']=='journal article') | (df['tipe']=='conference proceeding article') | (df['tipe']=='conference article') | (df['tipe']=='proceeding article') | (df['tipe']=='seminar article') | (df['tipe']=='procedia article')| (df['tipe']=='review') | (df['tipe']=='case report') | (df['tipe']=='peer review') | (df['tipe'].isnull())].reset_index(drop=True)
      df = df.reset_index(drop=True)
      df['id']=df.index+1
      df['judul_indo']=None #inisialisasi kolom judul_indo
      return df
      
    # CLEANING
    def translate_title(self,df):
      # translate judul to judul_indo
      df['judul_indo']=None
      for i in range(0,len(df)):
        if df['lang_judul'][i] == 'en' or (df['lang_judul'][i] != 'id' and df['lang_abstrak'][i] == 'en'):
          try:
            df['judul_indo'][i] = translator.translate(df['judul'][i] , dest='id').text
          except Exception:
            df['judul_indo'][i] = None
      df['id']=df.index+1
      return df
    
    def merge_data_duplicate(self,df):
      if df['author_sinta'].nunique()==1:
        author_sinta=df['author_sinta'][0]
      else:
        list_author_sinta=[]
        for i in range(0,len(df)):
          author_sinta = df['author_sinta'][i].split("; ")
          list_author_sinta.extend(author_sinta)
        list_author_sinta = list(set(list_author_sinta))
        list_author_sinta.sort()
        author_sinta = '; '.join(list_author_sinta)

      list_nama_dosen=[] # setelah match dosen
      for i in range(0,len(df)):
        nama_dosen = df['nama_dosen'][i].split("; ")
        list_nama_dosen.extend(nama_dosen) # setelah match dosen
      list_nama_dosen = list(set(list_nama_dosen))
      list_nama_dosen.sort()
      nama_dosen = '; '.join(list_nama_dosen) # setelah match dosen

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
          rank_paper = df['rank'][i].split(", ")
          rank.extend(rank_paper)
      rank = list(set(rank))
      rank.sort()
      rank=', '.join(rank)
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
      try:
        df = df.drop(columns=['is_dosen','author_dosen'], axis=1)
      except Exception:
        df = df
      df['id'] = df.index+1

      return df

    def cleaning_data(self,df,threshold):
      df = df.reset_index(drop=True)
      for i in range(0,len(df)):
        if df['doi'][i] == '':
          df['doi'][i]=None
      temp = df.link.fillna("0")
      df['sumber'] = np.where(temp.str.contains("scopus.com"),5,
                     np.where(temp.str.contains("webofscience.com"),4,
                     np.where(temp.str.contains("garuda.kemdikbud.go"),3,
                     np.where(temp.str.contains("scholar.google.com"),2,1))))    
      df = df.sort_values(by=['sumber','tahun'], ascending=False).reset_index(drop=True)
      df['id']=df.index+1
      # clean data by DOI
      df = self.clean_data_by_doi(df)
      
      # drop duplicate by judul, authors, nama_publikasi, dan tahun
      df = df.drop_duplicates(['judul','authors','nama_publikasi','tahun']).reset_index(drop=True)
      df = df.reset_index(drop=True)
      df['id']=df.index+1
      
      # 1st translate judul to judul_indo
      df_judul_indo_isnull = df.loc[df['judul_indo'].isnull()].reset_index(drop=True)
      df_judul_indo_notnull = df.loc[df['judul_indo'].notnull()].reset_index(drop=True)
      df_judul_indo_isnull = self.translate_title(df_judul_indo_isnull)
      df = pd.concat([df_judul_indo_notnull,df_judul_indo_isnull], ignore_index=True).sort_values(by=['id'])
      df = df.sort_values('doi').reset_index(drop=True)
      df['id'] = df.index+1 #reset kolom id
      
      # check for data with has similarity = 1
      df['list_ids'] = None
      df_doi_isnull = df[df['doi'].isnull()]
      if not df_doi_isnull.empty:
        list_ids=[]
        for i in range(df_doi_isnull.index[0], df_doi_isnull.index[-1]+1):
          ids=[]
          for j in range(0,len(df)):
            if i!=j:
              nama_dosen_i = df['nama_dosen'][i].split('; ')
              nama_dosen_j = df['nama_dosen'][j].split('; ')
              bool_set = set(nama_dosen_i) == set(nama_dosen_j)
              if (df['tahun'][i]==df['tahun'][j]) and (bool_set==True):
                pub_i = str(df['nama_publikasi'][i])
                pub_j = str(df['nama_publikasi'][j])
                if pub_i==None:
                  pub_i=''
                if pub_j==None:
                  pub_j=''
                  
                string1 = str(df['judul'][i])+' '+pub_i
                string2 = str(df['judul'][j])+' '+pub_j

                if df['lang_judul'][i] != 'id' and df['lang_judul'][j] == 'id':
                  if df['judul_indo'][i] !=None:
                    judul_i = str(df['judul_indo'][i])
                    string1 = judul_i+' '+pub_i
                elif df['lang_judul'][i] == 'id' and df['lang_judul'][j] != 'id':
                  if df['judul_indo'][j] !=None:
                    judul_j = str(df['judul_indo'][j])
                    string2 = judul_j+' '+pub_j

                lv_ratio = levenshtein.ratio(string1.lower(), string2.lower())
                string1 = str(df['judul'][i])+' '+pub_i

                if round(lv_ratio,2) == 1 and i!=j:
                  ids.append(df['id'][i])
                  ids.append(df['id'][j])
                  ids.sort()
                  list_ids.append(df['id'][i])
                  list_ids.append(df['id'][j])

          df['list_ids'][i] = list(set(ids))  #get id flag per row
        
        list_ids = list(set(list_ids))  #get all id with flag
        df_notflag = df[~df['id'].isin(list_ids)] #get id with not in list_id
        
        # merge data in list_ids
        df1 = pd.DataFrame()
        for i in range(df_doi_isnull.index[0], df_doi_isnull.index[-1]+1):
          if len(df['list_ids'][i])!=0:
            data = df[df['id'].isin(df['list_ids'][i])].reset_index(drop=True)
            data = self.merge_data_duplicate(data) # add self
            df1 = pd.concat([df1,data], ignore_index=True, sort=False)
            
        # join data hasil merge dan df_notflag 
        df = pd.concat([df_notflag,df1], ignore_index=True, sort=False)
        df = df.drop_duplicates('id').reset_index(drop=True) #drop duplicate by id
      
      df = df.sort_values('id') #sort data by id
      df = df.drop(columns=['list_ids'], axis=1)
      df = df.reset_index(drop=True)
      df['id'] = df.index+1 #reset kolom id
      
      # check data redundant
      df['id_paper'] = None
      df['list_ids'] = None
      df['flag'] = None
      df['group_data']=None
      
      # 2nd translate judul to judul_indo
      df_judul_indo_isnull = df.loc[df['judul_indo'].isnull()].reset_index(drop=True)
      df_judul_indo_notnull = df.loc[df['judul_indo'].notnull()].reset_index(drop=True)
      df_judul_indo_isnull = self.translate_title(df_judul_indo_isnull)
      df = pd.concat([df_judul_indo_notnull,df_judul_indo_isnull], ignore_index=True).sort_values(by=['id'])
      df = df.sort_values('doi').reset_index(drop=True)
      df['id'] = df.index+1 #reset kolom id
          
      # check for similarity >= threshold
      df_doi_isnull = df[df['doi'].isnull()]
      if not df_doi_isnull.empty:
        for i in range(df_doi_isnull.index[0], df_doi_isnull.index[-1]+1):
          list_id=[]
          list_doi=[]
          list_ratio=[]
          ids=[]
          for j in range(0,len(df)):
            if i!=j:
              nama_dosen_i = df['nama_dosen'][i].split('; ')
              nama_dosen_j = df['nama_dosen'][j].split('; ')
              bool_set = set(nama_dosen_i) == set(nama_dosen_j)
              if (df['tahun'][i]==df['tahun'][j]) and (bool_set==True):
                pub_i = str(df['nama_publikasi'][i])
                pub_j = str(df['nama_publikasi'][j])
                if pub_i==None:
                  pub_i=''
                if pub_j==None:
                  pub_j=''
                  
                string1 = str(df['judul'][i])+' '+pub_i
                string2 = str(df['judul'][j])+' '+pub_j

                if df['lang_judul'][i] != 'id' and df['lang_judul'][j] == 'id':
                  if df['judul_indo'][i] != None:
                    judul_i = str(df['judul_indo'][i])
                    string1 = judul_i+' '+pub_i
                elif df['lang_judul'][i] == 'id' and df['lang_judul'][j] != 'id':
                  if df['judul_indo'][j] != None:
                    judul_j = str(df['judul_indo'][j])
                    string2 = judul_j+' '+pub_j

                lv_ratio = levenshtein.ratio(string1.lower(), string2.lower())
                string1 = str(df['judul'][i])+' '+pub_i

                if lv_ratio >= threshold and i!=j:
                  if len(list_id)!=0:
                    if list_doi[-1]!=None and df['doi'][j]!=None:
                      if lv_ratio > list_ratio[-1]:
                        # delete last element list
                        del list_id[-1]
                        del list_doi[-1]
                        del list_ratio[-1]
                        # append new element
                        id=[]
                        id.append(df['id'][i])
                        id.append(df['id'][j])
                        id.sort()
                        list_id.append(id)
                        list_doi.append(df['doi'][j])
                        list_ratio.append(lv_ratio)
                      else:
                        # not append new element
                        list_id = list_id
                        list_doi = list_doi
                        list_ratio = list_ratio
                    # append new element
                    else:
                      id=[]
                      id.append(df['id'][i])
                      id.append(df['id'][j])
                      id.sort()
                      list_id.append(id)
                      list_doi.append(df['doi'][j])
                      list_ratio.append(lv_ratio)
                  # append new element
                  else:
                    id=[]
                    id.append(df['id'][i])
                    id.append(df['id'][j])
                    id.sort()
                    list_id.append(id)
                    list_doi.append(df['doi'][j])
                    list_ratio.append(lv_ratio)
          if len(list_id)==0:
            list_id=None
            list_ratio=None
          else:
            list_ratio = [ '%.2f' % elem for elem in list_ratio ] #round lv_ratio in list
          df['id_paper'][i] = list_id  #add list_id to column dataframe
          df['flag'][i] = list_ratio  #add list_id to column dataframe
        
        # add id_paper and flag to data DOI notnull
        for i in range(df_doi_isnull.index[0], df_doi_isnull.index[-1]+1):
          list_id_paper = df['id_paper'][i]
          if list_id_paper!=None:
            for id in list_id_paper:
              index = id[0]-1
              if df['id_paper'][index]==None:
                df['id_paper'][index] = df['id_paper'][i]  # add id_paper to first list id (doi notnull)
                df['flag'][index] = df['flag'][i]  # add flag to first list id (doi notnull)
        
        # add group data to column group_data 
        df['first_id'] = None
        df_id_paper_notnull = df[df['id_paper'].notnull()].reset_index(drop=True)
        df_id_paper_isnull = df[df['id_paper'].isnull()].reset_index(drop=True)
        if not df_id_paper_notnull.empty:
          for i in range(0,len(df_id_paper_notnull)):
            df_id_paper_notnull['first_id'][i] = df_id_paper_notnull['id_paper'][i][0][0]
          df_id_paper_notnull['group_data'] = df_id_paper_notnull.groupby(list(df_id_paper_notnull['first_id'])).ngroup()+1
        
        # concat df_id_paper_notnull and df_id_paper_isnull
        df = pd.concat([df_id_paper_notnull,df_id_paper_isnull], ignore_index=True)
        
        # filter df_id_paper
        df_id_paper_notnull = df[df['id_paper'].notnull()]
        df_id_paper_isnull = df[df['id_paper'].isnull()]
        
        # cover list_id and flag to string with ";" delimiter
        if not df_id_paper_notnull.empty:
          df_id_paper_notnull['id_paper'] = [';'.join(map(str, l)) for l in df_id_paper_notnull['id_paper']]
          df_id_paper_notnull['flag'] = [';'.join(map(str, l)) for l in df_id_paper_notnull['flag']]
        
        # concat df_id_paper_notnull and df_id_paper_isnull
        df = pd.concat([df_id_paper_notnull,df_id_paper_isnull], ignore_index=True)
        df = df.drop(columns=['first_id'], axis=1)
        
      df = df.drop(columns=['list_ids','sumber'], axis=1)
      df = df.sort_values(['group_data','doi']).reset_index(drop=True)

      return df
    
    def check_redundant_data(self,df,threshold):
      df['id_paper'] = None
      df['list_ids'] = None
      df['flag'] = None
      df['group_data']=None
      
      # translate judul 
      df_judul_indo_isnull = df.loc[df['judul_indo'].isnull()].reset_index(drop=True)
      df_judul_indo_notnull = df.loc[df['judul_indo'].notnull()].reset_index(drop=True)
      for i in range(0,len(df_judul_indo_isnull)):
        if (df_judul_indo_isnull['lang_judul'][i] == 'en') or (df_judul_indo_isnull['lang_judul'][i] != 'id' and df_judul_indo_isnull['lang_abstrak'][i] == 'en'):
          try:
            df_judul_indo_isnull['judul_indo'][i] = translator.translate(df_judul_indo_isnull['judul'][i] , dest='id').text
          except Exception:
            df_judul_indo_isnull['judul_indo'][i] = None
      df = pd.concat([df_judul_indo_notnull,df_judul_indo_isnull], ignore_index=True).sort_values(by=['id'])
      df = df.sort_values('doi').reset_index(drop=True)
      df['id'] = df.index+1 #reset kolom id
      
      # check for similarity >= threshold
      df_doi_isnull = df[df['doi'].isnull()]
      if not df_doi_isnull.empty:
        for i in range(df_doi_isnull.index[0], df_doi_isnull.index[-1]+1):
          list_id=[]
          list_doi=[]
          list_ratio=[]
          ids=[]
          for j in range(0,len(df)):
            if i!=j:
              nama_dosen_i = df['nama_dosen'][i].split('; ')
              nama_dosen_j = df['nama_dosen'][j].split('; ')
              bool_set = set(nama_dosen_i) == set(nama_dosen_j)
              if (df['tahun'][i]==df['tahun'][j]) and (bool_set==True):
                pub_i = str(df['nama_publikasi'][i])
                pub_j = str(df['nama_publikasi'][j])
                if pub_i==None:
                  pub_i=''
                if pub_j==None:
                  pub_j=''
                  
                string1 = str(df['judul'][i])+' '+pub_i
                string2 = str(df['judul'][j])+' '+pub_j

                if df['lang_judul'][i] != 'id' and df['lang_judul'][j] == 'id':
                  if df['judul_indo'][i] != None:
                    judul_i = str(df['judul_indo'][i])
                    string1 = judul_i+' '+pub_i
                elif df['lang_judul'][i] == 'id' and df['lang_judul'][j] != 'id':
                  if df['judul_indo'][j] != None:
                    judul_j = str(df['judul_indo'][j])
                    string2 = judul_j+' '+pub_j

                lv_ratio = levenshtein.ratio(string1.lower(), string2.lower())
                string1 = str(df['judul'][i])+' '+pub_i

                if lv_ratio >= threshold and i!=j:
                  if len(list_id)!=0:
                    if list_doi[-1]!=None and df['doi'][j]!=None:
                      if lv_ratio > list_ratio[-1]:
                        # delete last element list
                        del list_id[-1]
                        del list_doi[-1]
                        del list_ratio[-1]
                        # append new element
                        id=[]
                        id.append(df['id'][i])
                        id.append(df['id'][j])
                        id.sort()
                        list_id.append(id)
                        list_doi.append(df['doi'][j])
                        list_ratio.append(lv_ratio)
                      else:
                        # not append new element
                        list_id = list_id
                        list_doi = list_doi
                        list_ratio = list_ratio
                    # append new element
                    else:
                      id=[]
                      id.append(df['id'][i])
                      id.append(df['id'][j])
                      id.sort()
                      list_id.append(id)
                      list_doi.append(df['doi'][j])
                      list_ratio.append(lv_ratio)
                  # append new element
                  else:
                    id=[]
                    id.append(df['id'][i])
                    id.append(df['id'][j])
                    id.sort()
                    list_id.append(id)
                    list_doi.append(df['doi'][j])
                    list_ratio.append(lv_ratio)
          if len(list_id)==0:
            list_id=None
            list_ratio=None
          else:
            list_ratio = [ '%.2f' % elem for elem in list_ratio ] #round lv_ratio in list
          df['id_paper'][i] = list_id  #add list_id to column dataframe
          df['flag'][i] = list_ratio  #add list_id to column dataframe
        
        # add id_paper and flag to data DOI notnull
        for i in range(df_doi_isnull.index[0], df_doi_isnull.index[-1]+1):
          list_id_paper = df['id_paper'][i]
          if list_id_paper!=None:
            for id in list_id_paper:
              index = id[0]-1
              if df['id_paper'][index]==None:
                df['id_paper'][index] = df['id_paper'][i]  # add id_paper to first list id (doi notnull)
                df['flag'][index] = df['flag'][i]  # add flag to first list id (doi notnull)
        
        # add group data to column group_data 
        df['first_id'] = None
        df_id_paper_notnull = df[df['id_paper'].notnull()].reset_index(drop=True)
        df_id_paper_isnull = df[df['id_paper'].isnull()].reset_index(drop=True)
        if not df_id_paper_notnull.empty:
          for i in range(0,len(df_id_paper_notnull)):
            df_id_paper_notnull['first_id'][i] = df_id_paper_notnull['id_paper'][i][0][0]
          df_id_paper_notnull['group_data'] = df_id_paper_notnull.groupby(list(df_id_paper_notnull['first_id'])).ngroup()+1
        
        # concat df_id_paper_notnull and df_id_paper_isnull
        df = pd.concat([df_id_paper_notnull,df_id_paper_isnull], ignore_index=True)
        
        # filter df_id_paper
        df_id_paper_notnull = df[df['id_paper'].notnull()]
        df_id_paper_isnull = df[df['id_paper'].isnull()]
        
        # cover list_id and flag to string with ";" delimiter
        if not df_id_paper_notnull.empty:
          df_id_paper_notnull['id_paper'] = [';'.join(map(str, l)) for l in df_id_paper_notnull['id_paper']]
          df_id_paper_notnull['flag'] = [';'.join(map(str, l)) for l in df_id_paper_notnull['flag']]
        
        # concat df_id_paper_notnull and df_id_paper_isnull
        df = pd.concat([df_id_paper_notnull,df_id_paper_isnull], ignore_index=True)
        
        df = df.drop(columns=['list_ids','first_id'], axis=1)
        df = df.sort_values(['group_data','doi']).reset_index(drop=True)

      return df

    def merge_data(self,df):
      df = df.reset_index(drop=True)
      temp = df.link.fillna("0")
      df['sumber'] = np.where(temp.str.contains("scopus.com"),5,
                     np.where(temp.str.contains("webofscience.com"),4,
                     np.where(temp.str.contains("garuda.kemdikbud.go"),3,
                     np.where(temp.str.contains("scholar.google.com"),2,1))))    
      df = df.sort_values(by=['sumber','tahun'], ascending=False).reset_index(drop=True)
      id = df['id'][0]
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
        list_author_sinta = list(set(list_author_sinta))
        list_author_sinta.sort()
        author_sinta = '; '.join(list_author_sinta)

      list_nama_dosen=[] # setelah match dosen
      for i in range(0,len(df)):
        nama_dosen = df['nama_dosen'][i].split("; ")
        list_nama_dosen.extend(nama_dosen) # setelah match dosen
      list_nama_dosen = list(set(list_nama_dosen))
      list_nama_dosen.sort()
      nama_dosen = '; '.join(list_nama_dosen) # setelah match dosen

      judul = df['judul'][0]
      lang_judul = df['lang_judul'][0]
      link = df['link'][0]
      jumlah_sitasi = df.loc[df['jumlah_sitasi'].notnull()]['jumlah_sitasi'].max()
      
      doi=None
      doi_info=None
      authors=None
      for i in range(0, len(df)):
        if df['doi'].notnull()[i]:
          doi = df['doi'][i]
          doi_info = df['doi_info'][i]
          authors = df['authors'][i]
          break
        else:
          continue
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
          rank_paper = df['rank'][i].split(", ")
          rank.extend(rank_paper)
      rank = list(set(rank))
      rank.sort()
      rank=', '.join(rank)
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

      judul_indo = None
      for i in range(0, len(df)):
        if df['judul_indo'].notnull()[i]:
          judul_indo = df['judul_indo'][i]
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
      df['nama_dosen'] = [nama_dosen]
      df['judul_indo'] = [judul_indo]
      df['id_paper'] = [id_paper]
      df['flag'] = [flag]
      df['group_data'] = [group_data]

      return df
    
    def research_by_author(self, df):
      df['nama_dosen']=df['nama_dosen'].str.split('; ')
      df=df.explode('nama_dosen').reset_index(drop=True)
      return df
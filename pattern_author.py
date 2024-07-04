import re

class PatternAuthor(object):
    # nama lengkap
    def pattern1(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        fullname = author.title()
        return fullname

    # nama lengkap (kapital semua)
    def pattern2(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.upper()
        return author

    # huruf pertama nama depan kapital(titik) nama belakang: P. Sihombing --> PARDOMUAN ROBINSON SIHOMBING
    # jika satu kata: dua kali nama: Sukim Sukim --> SUKIM
    def pattern3(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.title()
        split_word = author.split()
        last_word = split_word[-1]
        first_word = split_word[0]
        first_word = re.sub('[^A-Z]', '', first_word)
        if len(author.split()) > 1:
            author_pattern = first_word+". "+last_word
        else:
            author_pattern = author+" "+author
        return author_pattern

    # nama belakang(koma) nama depan nama tengah dst: Wijayanto, Arie Wahyu --> ARIE WAHYU WIJAYANTO
    # jika satu kata: nama, nama: Takdir, Takdir --> TAKDIR
    def pattern4(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.title()
        split_word = author.split()
        last_word = split_word[-1]
        next_word = author.rsplit(' ', 1)[0]
        author_pattern = last_word+", "+next_word
        return author_pattern

    # huruf pertama nama depan(titik) huruf pertama nama tengah(titik) dst nama belakang: T. H. Siagian --> TIODORA HADUMAON SIAGIAN
    # jika satu kata: huruf pertama nama(titik) nama: N. Nasrudin --> NASRUDIN
    def pattern5(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.title()
        temp = list(re.sub('[^A-Z]', '', author.title().rsplit(' ', 1)[0]))
        temp.append(author.split()[-1])
        author_pattern = '. '.join(temp)
        return author_pattern

    # huruf pertama nama depanhuruf pertama nama tengah dst nama belakang: PR Sihombing --> PARDOMUAN ROBINSON SIHOMBING
    # jika satu kata: huruf kapital nama depan nama: N Nasrudin --> NASRUDIN
    def pattern6(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.title()
        split_word = author.split()
        last_word = split_word[-1]
        first_word = author.rsplit(' ', 1)[0]
        first_word = re.sub('[^A-Z]', '', first_word)
        if len(author.split()) == 1:
            first_word = re.sub('[^A-Z]', '', author)
            author_pattern = first_word+" "+author
        else:
            author_pattern = first_word+" "+last_word
        return author_pattern

    # huruf pertama nama depanhuruf pertama nama tengah dst nama belakang (kapital semua): PR SIHOMBING --> PARDOMUAN ROBINSON SIHOMBING
    # jika satu kata: huruf pertama nama nama (kapital semua): N NASRUDIN --> NASRUDIN
    def pattern7(self,author_pattern6):
        return author_pattern6.upper()

    # jika satu kata : nama (title)
    # jika 2 suku kata : huruf pertama nama belakang nama depan dst: P Setia --> SETIA PRAMANA
    # jika >2 suku kata : huruf pertama nama belakanghuruf pertama nama depan nama tengah dst
    def pattern8(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.title()
        split_word = author.split()
        middle_word = split_word[1:-1]
        first_word = split_word[0]
        last_word = split_word[-1]
        if len(author.split())==1:
            author_pattern = author
        elif len(author.split())==2:
            author_pattern = re.sub('[^A-Z]', '', last_word)+" "+first_word
        else:
            first_pattern = last_word+" "+first_word
            first_word = re.sub('[^A-Z]', '', first_pattern)
            author_pattern = first_word+" "+' '.join(middle_word)
        return author_pattern

    # nama belakang(koma) huruf kapital nama depan nama tengah dst: Wijayanto, AW --> ARIE WAHYU WIJAYANTO
    # satu kata: Sukim, --> SUKIM
    def pattern9(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.title()
        split_word = author.split()
        last_word = split_word[-1]
        second = re.sub('[^A-Z]', '', author)[:-1]
        if len(author.split()) == 1:
            author_pattern = author+','
        else:
            author_pattern = last_word+", "+second
        return author_pattern

    # huruf pertama semua kata (kapital semua): EDU --> EFRI DIAH UTAMI
    def pattern10(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.title()
        author_pattern = re.sub('[^A-Z]', '', author)
        return author_pattern
    
    # nama depan nama belakang: Tiodora Siagian --> TIODORA HADUMAON SIAGIAN; Sukim Sukim
    def pattern11(self,author):
        author = re.sub(' +', ' ', author).rstrip()
        author = author.title()
        split_word = author.split()
        author_pattern = split_word[0]+' '+split_word[-1]
        return author_pattern

    def pattern_dosen(self,df,col_nama):
        df['pattern1']=df.apply(lambda x: self.pattern1(x[col_nama]), axis=1)
        df['pattern2']=df.apply(lambda x: self.pattern2(x[col_nama]), axis=1)
        df['pattern3']=df.apply(lambda x: self.pattern3(x[col_nama]), axis=1)
        df['pattern4']=df.apply(lambda x: self.pattern4(x[col_nama]), axis=1)
        df['pattern5']=df.apply(lambda x: self.pattern5(x[col_nama]), axis=1)
        df['pattern6']=df.apply(lambda x: self.pattern6(x[col_nama]), axis=1)
        df['pattern7']=df.apply(lambda x: self.pattern7(x['pattern6']), axis=1)
        df['pattern8']=df.apply(lambda x: self.pattern8(x[col_nama]), axis=1)
        df['pattern9']=df.apply(lambda x: self.pattern9(x[col_nama]), axis=1)
        df['pattern10']=df.apply(lambda x: self.pattern10(x[col_nama]), axis=1)
        df['pattern11']=df.apply(lambda x: self.pattern11(x[col_nama]), axis=1)
        return df

# Clone this repository in your folder
```sh
git clone https://git.stis.ac.id/Luthfim/skripsi_upi.git
```
# Python version for REST API
Python 3.8 and newer

## Create virtual environment

Flask [installation](https://flask.palletsprojects.com/en/3.0.x/installation/) 

### Create environment in Windows
```sh
mkdir myproject
cd myproject
py -3 -m venv .venv
```
Activate the environment
```sh
.venv\Scripts\activate
```
Install Flask
```sh
pip install Flask
```
Install requirements
```sh
pip install -r requirements.txt
```
### Move all file and folder in folder rest_api to folder myproject
Your folder structure
```
myproject/
├──venv
├── static
│   └── data     
│       ├── clean
│       ├── garuda
│       ├── google
│       ├── integrasi
│       ├── scopus
│       └── wos
├── config.py 
├── integration.py 
├── main.py 
├── pattern_author.py 
├── requirements.txt  
└── sinta_scraper.py 
```
### Running flask app
```sh
flask --app main.py --debug run
```

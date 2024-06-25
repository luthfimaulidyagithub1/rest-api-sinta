## Python version for REST API

Python 3.8 and newerr

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

Install requirements

```sh
pip install -r requirements.txt
```

## Clone this repository in folder myproject

```sh
git clone https://github.com/luthfimaulidyagithub1/rest-api-sinta.git
```

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

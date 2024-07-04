## Python version for REST API

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

### Edit config with your API KEY

- [API ELSEVIER](https://dev.elsevier.com/)
- [API SEMANTIC SCHOLAR](https://www.semanticscholar.org/product/api#api-key-form)
- [API SPRINGER](https://dev.springernature.com/)

```sh
API_KEY_ELSEVIER = 'YOUR API KEY'
API_KEY_SEMANTIC = 'YOUR API KEY'
API_KEY_SPRINGER = 'YOUR API KEY'
```

### Running flask app

```sh
flask --app main.py --debug run
```

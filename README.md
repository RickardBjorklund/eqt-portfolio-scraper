# EQT Portfolio Scraper
Scrapes company data from EQT's public website and enriches it with information provided by the Motherbrain Team.

# Prerequisites
To run this code you will need **Python 3.12**.

# Install - Linux / MacOS
Create a virtual environment `python3 -m venv venv`, activate it `source venv/bin/activate` and install requirements `python3 -m pip install -r requirements.txt`.

# Install - Windows
Create a virtual environment `python -m venv venv`, activate it `.\venv\Scripts\activate.bat` and install requirements `python -m pip install -r requirements.txt`.

# Install - Playwright
In order to run the code with the `--use-html-scraper` argument, you need to let Playwright install the required browsers first `playwright install`.

# Basic usage
To run the code in its most basic form: `python src/main.py`.
To see all options available when running code: `python src/main.py --help`.
To run unittests: `python -m unittest tests/test_page_data.py`

# Resulting data
The code will produce a JSON file named `result_{current_time}.json` and place it in the `/results` folder.
The `/results` folder also contains data scraped and enriched with the code using a few different approaches reflected in the respective file names.

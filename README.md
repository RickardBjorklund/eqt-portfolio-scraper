# EQT Portfolio Scraper
Scrapes company data from EQT's public website and enriches it with information provided by the Motherbrain Team.

# Install - Linux / MacOS
Create a virtual environment `python3 -m venv venv`, activate it `source venv/bin/activate` and install requirements `python3 -m pip install -r requirements.txt`.

# Install - Windows
Create a virtual environment `python -m venv venv`, activate it `.\venv\Scripts\activate.bat` and install requirements `python -m pip install -r requirements.txt`.

# Install - Playwright
In order to run the code with the `--use-html-scraper` argument, you need to let Playwright install the required browsers `playwright install`.

# Basic usage
To run the code in its most basic form: `python main.py`.
To see all options available when running code: `python main.py --help`.

## Resulting data
The code will produce a JSON file named `result.json` and place it in the `/results` folder.

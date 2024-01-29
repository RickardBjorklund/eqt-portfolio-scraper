import argparse
import asyncio
import time

import pandas

from gcs_client import fetch_enrichment_data
from page_data.page_data_sync import get_registered_domain
from utils import time_function, save_df_to_file


@time_function
def get_company_data_from_html(args):
    if args.synchronous:
        from html_playwright.playwright_sp import scrape_website
    else:
        from html_playwright.playwright_mp import scrape_website

    company_data = []
    if "current" in args.type:
        print("current html")
        company_data += scrape_website("https://eqtgroup.com/current-portfolio/")
    if "divested" in args.type:
        print("divested html")
        company_data += scrape_website("https://eqtgroup.com/current-portfolio/divestments/")

    for company in company_data:
        company["registered_domain"] = get_registered_domain(company.get("web"))

    return company_data


@time_function
def get_company_data_from_page_data(portfolio_type):
    from page_data.page_data_sync import fetch_companies, get_company_data

    companies = []
    if "current" in portfolio_type:
        companies += fetch_companies("https://eqtgroup.com/page-data/current-portfolio/page-data.json")
    if "divested" in portfolio_type:
        companies += fetch_companies("https://eqtgroup.com/page-data/current-portfolio/divestments/page-data.json")

    company_data = get_company_data(companies)

    return company_data


@time_function
def get_company_data_from_page_data_async(portfolio_type):
    from page_data.page_data_async import fetch_companies, get_company_data

    companies = []
    if "current" in portfolio_type:
        companies += asyncio.run(fetch_companies("https://eqtgroup.com/page-data/current-portfolio/page-data.json"))
    if "divested" in portfolio_type:
        companies += asyncio.run(
            fetch_companies("https://eqtgroup.com/page-data/current-portfolio/divestments/page-data.json"))

    company_data = asyncio.run(get_company_data(companies))

    return company_data


@time_function
def get_funding_rounds(companies_data):
    funding_rounds = fetch_enrichment_data("interview-test-funding.json.gz")
    print(f"{len(funding_rounds.index)} funding round entries were loaded from CGS")

    result = []
    for company in companies_data.itertuples():
        company_uuid = company.uuid
        if not isinstance(company_uuid, str):
            result.append([])
            continue

        rounds = funding_rounds.query(f'org_uuid == "{company_uuid}"')
        if rounds.empty:
            result.append([])
            continue

        result.append(
            rounds[["investor_count", "announced_on", "investment_type", "raised_amount_usd"]].to_dict("records"))

    return result


@time_function
def main():
    parser = argparse.ArgumentParser(
        prog="EQT Portfolio Scraper", description="Scrapes company data from EQT's public website and enriches it with "
                                                  "information provided by the Motherbrain Team")

    parser.add_argument("-t", "--type", dest="type", choices=["current", "divested"],
                        default=["current", "divested"],
                        help="Chose to only get current or divested companies, leave out argument to get both")
    parser.add_argument("--skip-funding-rounds", action="store_true",
                        help="Skip step where company data is enriched with information about their funding rounds")
    parser.add_argument("--use-html-scraper", action="store_true",
                        help="Use Playwright to scrape data from HTML instead of requesting JSON page data "
                             "(slower, slightly lower fidelity, made for demonstrative purposes)")
    parser.add_argument("--synchronous", action="store_true",
                        help="Same results, longer execution time, but easier to debug")

    args = parser.parse_args()

    if args.use_html_scraper:
        company_data = get_company_data_from_html(args)
    else:
        if args.synchronous:
            company_data = get_company_data_from_page_data(args.type)
        else:
            company_data = get_company_data_from_page_data_async(args.type)
    print(f"{len(company_data)} companies were found by scraping website")
    company_data_df = pandas.DataFrame.from_dict(company_data)

    all_organizations = fetch_enrichment_data("interview-test-org.json.gz")
    print(f"{len(all_organizations.index)} organization entries were loaded from CGS")

    start = time.perf_counter()
    all_organizations.dropna(subset=['homepage_url'], inplace=True)
    all_organizations['registered_domain'] = all_organizations['homepage_url'].apply(get_registered_domain)
    all_organizations.drop_duplicates(subset=["name", "registered_domain"], keep="last", inplace=True)
    print(f"Adding registered_domain and cleaning dataframe took {time.perf_counter() - start:.6f} seconds to execute")
    print(f"{len(all_organizations.index)} organization entries remain after cleaning")

    print(f"{len(company_data_df.index)} rows of company data before merge")
    start = time.perf_counter()
    enriched_company_data = pandas.merge(company_data_df, all_organizations, how="left",
                                         left_on=["registered_domain", "title"], right_on=["registered_domain", "name"])
    print(f"Merging DataFrames took {time.perf_counter() - start:.6f} seconds to execute")
    print(f"{len(enriched_company_data.index)} rows of company data after merge")

    if not args.skip_funding_rounds:
        funding_rounds = get_funding_rounds(enriched_company_data)
        enriched_company_data = enriched_company_data.assign(funding_rounds=funding_rounds)

    save_df_to_file(enriched_company_data)


if __name__ == "__main__":
    main()

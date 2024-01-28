import argparse
import time

import pandas

from gcs_client import fetch_enrichment_data
from page_data.shared import get_registered_domain
from utils import time_function, json_file_to_data_frame


@time_function
def get_company_data_from_html(args):
    if args.single_process:
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
def get_company_data_from_page_data(args):
    if args.single_process:
        from page_data.page_data_sp import fetch_companies, get_company_data
    else:
        from page_data.page_data_mp import fetch_companies, get_company_data

    companies = []
    if "current" in args.type:
        companies += fetch_companies("https://eqtgroup.com/page-data/current-portfolio/page-data.json")
    if "divested" in args.type:
        companies += fetch_companies("https://eqtgroup.com/page-data/current-portfolio/divestments/page-data.json")

    company_data = get_company_data(companies)

    return company_data


@time_function
def get_funding_rounds(companies_data):
    # funding_rounds = fetch_enrichment_data("interview-test-funding.json.gz")
    funding_rounds = json_file_to_data_frame("../real_data/funding_rounds.json")

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
    parser.add_argument("--single-process", action="store_true",
                        help="Longer execution time, less stress on CPU and easier to debug")

    args = parser.parse_args()

    if args.use_html_scraper:
        company_data = get_company_data_from_html(args)
    else:
        company_data = get_company_data_from_page_data(args)
    company_data_df = pandas.DataFrame.from_dict(company_data)

    # all_organizations = fetch_enrichment_data("interview-test-org.json.gz")
    all_organizations = json_file_to_data_frame("../real_data/organizations.json")

    start = time.perf_counter()
    all_organizations.dropna(subset=['homepage_url'], inplace=True)
    all_organizations['registered_domain'] = all_organizations['homepage_url'].apply(get_registered_domain)
    all_organizations.drop_duplicates(subset=["name", "registered_domain"], keep="last", inplace=True)
    print(f"Adding registered_domain and cleaning dataframe took {time.perf_counter() - start:.6f} seconds to execute")

    print(f"{len(company_data_df.index)} rows before merge")
    start = time.perf_counter()
    enriched_company_data = pandas.merge(company_data_df, all_organizations, how="left",
                                         left_on=["registered_domain", "title"], right_on=["registered_domain", "name"])
    end = time.perf_counter()
    elapsed = end - start
    print(f"Merging DataFrames took {elapsed:.6f} seconds to execute")
    print(f"{len(enriched_company_data.index)} rows after merge")

    if not args.skip_funding_rounds:
        funding_rounds = get_funding_rounds(enriched_company_data)
        enriched_company_data = enriched_company_data.assign(funding_rounds=funding_rounds)

    with open("../results/result3.json", "w", encoding="utf-8") as file:
        enriched_company_data.to_json(path_or_buf=file, orient="records", indent=4, force_ascii=False)


if __name__ == "__main__":
    main()

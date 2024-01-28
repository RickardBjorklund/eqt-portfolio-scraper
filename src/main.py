import time
import pandas
import requests
import tldextract

from gcs_client import fetch_enrichment_data
from utils import time_function, json_file_to_data_frame

BASE_URL = "https://eqtgroup.com/page-data"


def get_registered_domain(url):
    if not isinstance(url, str):
        return None
    return tldextract.extract(url).registered_domain


def get_logo_url(logo):
    if logo is None:
        return None
    if logo.get("asset") is None:
        return None
    return logo.get("asset").get("url")


@time_function
def fetch_companies(url):
    try:
        response = requests.get(url).json()
        return response["result"]["data"]["allSanityCompanyPage"]["nodes"]
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def fetch_company_details(url):
    try:
        response = requests.get(url).json()
        return response["result"]["data"]["sanityCompanyPage"]
    except:
        return None


def get_description(raw_body):
    if not raw_body:
        return None
    description = ""
    for part in raw_body:
        for sub_part in part.get("children", []):
            description += sub_part.get("text", "")  # TODO: Maybe add space or newline after each sub_part
    return description


def extract_details(company_details):
    interesting_details = {
        "description": get_description(company_details.get("_rawBody", [])),
        "preamble": company_details.get("preamble"),
        "heading": company_details.get("heading"),
        "responsibleAdvisors": [advisor.get("title") for advisor in company_details.get("responsibleAdvisors", [])],
        "website": company_details.get("website"),
        "registeredDomain": get_registered_domain(company_details.get("website")),
        "board": company_details.get("board", []),
        "management": company_details.get("management", []),
        "logo": get_logo_url(company_details.get("logo")),
    }

    # This data exists on the details page, but we already have it from the company listing
    # redundant_details = {
    #     "title": company_details.get("title", ""),
    #     "country": company_details.get("country", ""),
    #     "entryDate": company_details.get("entryDate", ""),
    #     "exitDate": company_details.get("exitDate", ""),
    #     "sector": company_details.get("sector", ""),
    #     "fund": [fund.get("title", "") for fund in company_details.get("fund", [])],
    # }

    return interesting_details


@time_function
def get_company_data(companies):
    companies_data = []
    for company in companies:
        company_data = {
            "title": company.get("title"),
            "country": company.get("country"),
            "entryDate": company.get("entryDate"),
            "exitDate": company.get("exitDate"),
            "fund": [fund.get("title", "") for fund in company.get("fund", [])],
            "sector": company.get("sector"),
        }

        # promotedSdg, sdg and topic exist in the page data,
        # but are always (with one exception where promotedSdg=3) undefined.
        # The path to the company details site on EQT webpage doesn't really seem useful in this context.
        # The id seems useless in this context as it is not correlating to the id in the data from gcs.
        # unused_company_data = {
        #     "promotedSdg": company.get("promotedSdg"),
        #     "sdg": company.get("sdg"),
        #     "topic": company.get("topic"),
        #     "path": company.get("path"),
        #     "_id": company.get("_id"),
        # }

        company_details = fetch_company_details(f"{BASE_URL}{company.get("path")}page-data.json")
        if not company_details:
            companies_data.append(company_data)
            print(f"No details could be found for: {company.get("title")}")
            continue

        interesting_details = extract_details(company_details)
        companies_data.append(company_data | interesting_details)

    return companies_data


@time_function
def get_funding_rounds(companies_data):
    funding_rounds = fetch_enrichment_data("interview-test-funding.json.gz")
    # funding_rounds = json_file_to_data_frame("real_data/funding_rounds.json")

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
    current_portfolio_companies = fetch_companies(f"{BASE_URL}/current-portfolio/page-data.json")
    divestment_companies = fetch_companies(f"{BASE_URL}/current-portfolio/divestments/page-data.json")

    company_data = get_company_data(current_portfolio_companies + divestment_companies)
    company_data_df = pandas.DataFrame.from_dict(company_data)

    all_organizations = fetch_enrichment_data("interview-test-org.json.gz")
    # all_organizations = json_file_to_data_frame("real_data/organizations.json")

    start = time.perf_counter()
    all_organizations.dropna(subset=['homepage_url'], inplace=True)
    all_organizations['registeredDomain'] = all_organizations['homepage_url'].apply(get_registered_domain)
    all_organizations.drop_duplicates(subset=["name", "registeredDomain"], keep="last", inplace=True)
    print(f"Adding registeredDomain and cleaning dataframe took {time.perf_counter() - start:.6f} seconds to execute")

    print(f"{len(company_data_df.index)} rows before merge")
    start = time.perf_counter()
    enriched_company_data = pandas.merge(company_data_df, all_organizations, how="left",
                                         left_on=["registeredDomain", "title"], right_on=["registeredDomain", "name"])
    end = time.perf_counter()
    elapsed = end - start
    print(f"Merging DataFrames took {elapsed:.6f} seconds to execute")
    print(f"{len(enriched_company_data.index)} rows after merge")

    funding_rounds = get_funding_rounds(enriched_company_data)
    enriched_company_data_with_funding_rounds = enriched_company_data.assign(funding_rounds=funding_rounds)

    with open("result.json", "w", encoding="utf-8") as file:
        enriched_company_data_with_funding_rounds.to_json(path_or_buf=file, orient="records", indent=4,
                                                          force_ascii=False)


if __name__ == '__main__':
    main()

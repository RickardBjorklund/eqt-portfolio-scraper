import multiprocessing as mp

from page_data.shared import extract_details, fetch_companies, fetch_company_details


def get_company_data_singular(company):
    company_data = {
        "title": company.get("title"),
        "sector": company.get("sector"),
        "country": company.get("country"),
        "fund": [fund.get("title", "") for fund in company.get("fund", [])],
        "entry": company.get("entryDate"),
        "exit": company.get("exitDate"),
        "company_details_path": company.get("path"),
    }

    # promotedSdg, sdg and topic exist in the page data,
    # but are always (with one exception where promotedSdg=3) undefined.
    # The id doesn't really seem useful in this context as it is not correlating to the id in the data from gcs.
    # unused_company_data = {
    #     "promotedSdg": company.get("promotedSdg"),
    #     "sdg": company.get("sdg"),
    #     "topic": company.get("topic"),
    #     "_id": company.get("_id"),
    # }

    company_details = fetch_company_details(f"https://eqtgroup.com/page-data{company.get("path")}page-data.json")
    if not company_details:
        print(f"No details could be found for: {company.get("title")}")
        return company_data
    else:
        interesting_details = extract_details(company_details)
        return company_data | interesting_details


def get_company_data(companies):
    with mp.Pool(processes=mp.cpu_count()) as pool:
        res = pool.map(get_company_data_singular, companies)

    return res

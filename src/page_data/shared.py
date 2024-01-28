import requests
import tldextract


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
        "responsible_advisors": [advisor.get("title") for advisor in company_details.get("responsibleAdvisors", [])],
        "website": company_details.get("website"),
        "registered_domain": get_registered_domain(company_details.get("website")),
        "board": company_details.get("board", []),
        "management": company_details.get("management", []),
        "logo": get_logo_url(company_details.get("logo")),
    }

    # This data exists on the details page, but also in the company listing
    # redundant_details = {
    #     "title": company_details.get("title", ""),
    #     "country": company_details.get("country", ""),
    #     "entry": company_details.get("entryDate", ""),
    #     "exit": company_details.get("exitDate", ""),
    #     "sector": company_details.get("sector", ""),
    #     "fund": [fund.get("title", "") for fund in company_details.get("fund", [])],
    # }

    return interesting_details
